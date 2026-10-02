"""SQLite persistence layer. A single connection guarded by a lock keeps the
settlement recompute (read events -> rewrite trips/charges) atomic."""
from __future__ import annotations

import os
import sqlite3
import threading

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    event_id       TEXT PRIMARY KEY,
    passenger_id   TEXT NOT NULL,
    station_id     TEXT NOT NULL,
    tap_type       TEXT NOT NULL CHECK (tap_type IN ('in', 'out')),
    occurred_at    TEXT NOT NULL,          -- ISO-8601, normalised to UTC
    seq            INTEGER NOT NULL,       -- arrival order, tie-breaks equal timestamps
    status         TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('paired', 'pending')),
    pending_reason TEXT,
    -- Non-null while the event's complete payload is frozen under a tap-event
    -- review hold. Held events are excluded from pairing, billing and the
    -- pending list until the (immutable) hold is released; the event row itself
    -- is never rewritten by a hold (no FK: the marker is set before the hold
    -- row exists, and hold rows can never be deleted anyway).
    held_id        TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_passenger ON events (passenger_id, occurred_at, seq);
-- idx_events_held is created in Database.__init__ (after the additive
-- migration for databases that predate held_id), so SCHEMA can run unchanged
-- on both fresh and older files.

CREATE TABLE IF NOT EXISTS trips (
    trip_id       TEXT PRIMARY KEY,        -- "<in_event_id>:<out_event_id>"
    passenger_id  TEXT NOT NULL,
    in_event_id   TEXT NOT NULL,
    out_event_id  TEXT NOT NULL,
    from_station  TEXT NOT NULL,
    to_station    TEXT NOT NULL,
    from_zone     INTEGER NOT NULL,
    to_zone       INTEGER NOT NULL,
    started_at    TEXT NOT NULL,
    ended_at      TEXT NOT NULL,
    day_key       TEXT NOT NULL,           -- UTC date of tap-in, YYYY-MM-DD
    week_key      TEXT NOT NULL            -- ISO week of tap-in, e.g. 2026-W39
);
CREATE INDEX IF NOT EXISTS idx_trips_passenger ON trips (passenger_id, started_at);

CREATE TABLE IF NOT EXISTS trip_charges (
    trip_id                  TEXT PRIMARY KEY,
    passenger_id             TEXT NOT NULL,
    day_key                  TEXT NOT NULL,
    week_key                 TEXT NOT NULL,
    base_fare_cents          INTEGER NOT NULL,
    transfer_discount_cents  INTEGER NOT NULL,
    amount_cents             INTEGER NOT NULL,   -- final charge after caps
    basis                    TEXT NOT NULL       -- JSON: full billing basis
);
CREATE INDEX IF NOT EXISTS idx_charges_passenger ON trip_charges (passenger_id, day_key);

-- Tap-event review holds (刷卡事件复核暂存). A hold freezes the complete payload
-- of one or more of a passenger's events while a review runs. Each held event
-- gets an immutable payload snapshot row; the events' held_id marker removes
-- them from pairing, billing and the pending list. The hold row and its
-- snapshots are append-only: only the events.held_id marker is cleared on
-- release. Whether a hold is still held or already released is derived from
-- the existence of an immutable release row (version 1 = held, 2 = released).
CREATE TABLE IF NOT EXISTS event_holds (
    hold_id            TEXT PRIMARY KEY,
    payload_hash       TEXT NOT NULL,     -- sha256 of the canonical hold request
    payload            TEXT NOT NULL,     -- canonical JSON of the normalised request
    passenger_id       TEXT NOT NULL,
    review_reason      TEXT NOT NULL,
    event_count        INTEGER NOT NULL,
    result             TEXT NOT NULL,     -- JSON: the hold response body
    created_at         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS event_hold_snapshots (
    hold_id            TEXT NOT NULL REFERENCES event_holds(hold_id),
    seq                INTEGER NOT NULL,  -- 0-based, event_ids in canonical (sorted) order
    event_id           TEXT NOT NULL,
    passenger_id       TEXT NOT NULL,
    station_id         TEXT NOT NULL,
    tap_type           TEXT NOT NULL,
    occurred_at        TEXT NOT NULL,     -- ISO-8601, normalised to UTC
    event_seq          INTEGER NOT NULL,  -- the event's original arrival seq, preserved on release
    payload_hash       TEXT NOT NULL,     -- sha256 of this event's canonical payload
    frozen_at          TEXT NOT NULL,
    PRIMARY KEY (hold_id, seq)
);

-- Release outcomes are append-only: a hold is released at most once. The
-- expected version and the frozen per-event payload hashes gate the release;
-- a version conflict or snapshot divergence refuses and leaves events held.
-- The post-release settlement deltas are frozen onto the release row itself;
-- nothing ever rewrites a hold or a snapshot.
CREATE TABLE IF NOT EXISTS event_hold_releases (
    release_id            TEXT PRIMARY KEY,   -- "RL-<hold_id>"
    hold_id               TEXT NOT NULL UNIQUE REFERENCES event_holds(hold_id),
    payload_hash          TEXT NOT NULL,       -- sha256 of the canonical release request
    expected_version      INTEGER NOT NULL,    -- optimistic version the release was based on
    result                TEXT NOT NULL,       -- JSON: the release response body (frozen deltas)
    created_at            TEXT NOT NULL
);

CREATE TRIGGER IF NOT EXISTS trg_event_holds_no_update
BEFORE UPDATE ON event_holds
BEGIN
    SELECT RAISE(ABORT, 'event_holds is append-only: UPDATE is not allowed');
END;

CREATE TRIGGER IF NOT EXISTS trg_event_holds_no_delete
BEFORE DELETE ON event_holds
BEGIN
    SELECT RAISE(ABORT, 'event_holds is append-only: DELETE is not allowed');
END;

CREATE TRIGGER IF NOT EXISTS trg_event_hold_snapshots_no_update
BEFORE UPDATE ON event_hold_snapshots
BEGIN
    SELECT RAISE(ABORT, 'event_hold_snapshots are immutable: UPDATE is not allowed');
END;

CREATE TRIGGER IF NOT EXISTS trg_event_hold_snapshots_no_delete
BEFORE DELETE ON event_hold_snapshots
BEGIN
    SELECT RAISE(ABORT, 'event_hold_snapshots are immutable: DELETE is not allowed');
END;

CREATE TRIGGER IF NOT EXISTS trg_event_hold_releases_no_update
BEFORE UPDATE ON event_hold_releases
BEGIN
    SELECT RAISE(ABORT, 'event_hold_releases is append-only: UPDATE is not allowed');
END;

CREATE TRIGGER IF NOT EXISTS trg_event_hold_releases_no_delete
BEFORE DELETE ON event_hold_releases
BEGIN
    SELECT RAISE(ABORT, 'event_hold_releases is append-only: DELETE is not allowed');
END;

CREATE TABLE IF NOT EXISTS recon_batches (
    batch_id              TEXT PRIMARY KEY,
    payload_hash          TEXT NOT NULL,     -- sha256 of the canonical request
    window_start          TEXT NOT NULL,     -- ISO-8601, normalised to UTC
    window_end            TEXT NOT NULL,     -- half-open window on tap-out time
    tolerance_seconds     INTEGER NOT NULL,
    snapshot_trip_count   INTEGER NOT NULL,
    external_record_count INTEGER NOT NULL,
    created_at            TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS recon_results (
    batch_id                TEXT NOT NULL REFERENCES recon_batches(batch_id),
    seq                     INTEGER NOT NULL,
    category                TEXT NOT NULL,
    passenger_id            TEXT NOT NULL,
    trip_id                 TEXT,
    serial_no               TEXT,
    internal_amount_cents   INTEGER,
    external_amount_cents   INTEGER,
    difference_cents        INTEGER,
    internal_ended_at       TEXT,
    external_charged_at     TEXT,
    match_method            TEXT,
    reason                  TEXT NOT NULL,
    PRIMARY KEY (batch_id, seq)
);

-- Transfer network validations. A validation_id maps at most one request;
-- the canonical payload hash enforces idempotency (same payload replays the
-- stored result, different payload -> 409). Rows are append-only: a frozen
-- snapshot or a past validation must never be rewritten by a later call.
CREATE TABLE IF NOT EXISTS network_validations (
    validation_id   TEXT PRIMARY KEY,
    payload_hash    TEXT NOT NULL,     -- sha256 of the canonical request
    payload         TEXT NOT NULL,     -- canonical JSON of the normalised request
    effective_start TEXT NOT NULL,     -- ISO-8601, normalised to UTC
    effective_end   TEXT NOT NULL,     -- half-open window
    passed          INTEGER NOT NULL,  -- 1 = no conflict, 0 = conflict found
    result          TEXT NOT NULL,     -- JSON: the exact API response
    created_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS network_snapshots (
    snapshot_id   TEXT PRIMARY KEY,    -- the frozen validation_id
    payload       TEXT NOT NULL,       -- canonical JSON copied at freeze time
    result        TEXT NOT NULL,       -- validation response copied at freeze time
    created_at    TEXT NOT NULL
);

CREATE TRIGGER IF NOT EXISTS trg_network_validations_no_update
BEFORE UPDATE ON network_validations
BEGIN
    SELECT RAISE(ABORT, 'network_validations is append-only: UPDATE is not allowed');
END;

CREATE TRIGGER IF NOT EXISTS trg_network_validations_no_delete
BEFORE DELETE ON network_validations
BEGIN
    SELECT RAISE(ABORT, 'network_validations is append-only: DELETE is not allowed');
END;

CREATE TRIGGER IF NOT EXISTS trg_network_snapshots_no_update
BEFORE UPDATE ON network_snapshots
BEGIN
    SELECT RAISE(ABORT, 'network_snapshots is append-only: UPDATE is not allowed');
END;

CREATE TRIGGER IF NOT EXISTS trg_network_snapshots_no_delete
BEFORE DELETE ON network_snapshots
BEGIN
    SELECT RAISE(ABORT, 'network_snapshots is append-only: DELETE is not allowed');
END;

-- Fare disputes. A dispute row is written once when the dispute is opened and
-- is never updated: the open/resolved state and the optimistic version are
-- derived from whether an immutable refund row exists (version 1 = open,
-- version 2 = resolved). The charge snapshot freezes the trip receivable at
-- creation time; later event corrections recompute the live trip_charges row
-- but can never rewrite the frozen amount or the settled refund.
CREATE TABLE IF NOT EXISTS disputes (
    dispute_id            TEXT PRIMARY KEY,
    payload_hash          TEXT NOT NULL,     -- sha256 of the canonical request
    passenger_id          TEXT NOT NULL,
    trip_id               TEXT NOT NULL,
    claimed_amount_cents  INTEGER NOT NULL,  -- requested refund
    reason                TEXT NOT NULL,
    created_at            TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dispute_snapshots (
    dispute_id                 TEXT PRIMARY KEY REFERENCES disputes(dispute_id),
    trip_id                    TEXT NOT NULL,
    passenger_id               TEXT NOT NULL,
    amount_cents               INTEGER NOT NULL,  -- frozen receivable
    base_fare_cents            INTEGER NOT NULL,
    transfer_discount_cents    INTEGER NOT NULL,
    basis                      TEXT NOT NULL,     -- JSON: full billing basis
    frozen_at                  TEXT NOT NULL
);

-- Immutable refund ledger. Exactly one row per resolved dispute, written in
-- the same transaction that closes it; no later correction can overwrite it.
CREATE TABLE IF NOT EXISTS refunds (
    refund_id             TEXT PRIMARY KEY,     -- "RF-<dispute_id>"
    dispute_id            TEXT NOT NULL UNIQUE REFERENCES disputes(dispute_id),
    passenger_id          TEXT NOT NULL,
    trip_id               TEXT NOT NULL,
    amount_cents          INTEGER NOT NULL,     -- approved refund, immutable
    payload_hash          TEXT NOT NULL,        -- sha256 of the canonical resolve request
    review_note           TEXT,
    created_at            TEXT NOT NULL
);

CREATE TRIGGER IF NOT EXISTS trg_disputes_no_update
BEFORE UPDATE ON disputes
BEGIN
    SELECT RAISE(ABORT, 'disputes is append-only: UPDATE is not allowed');
END;

CREATE TRIGGER IF NOT EXISTS trg_disputes_no_delete
BEFORE DELETE ON disputes
BEGIN
    SELECT RAISE(ABORT, 'disputes is append-only: DELETE is not allowed');
END;

CREATE TRIGGER IF NOT EXISTS trg_dispute_snapshots_no_update
BEFORE UPDATE ON dispute_snapshots
BEGIN
    SELECT RAISE(ABORT, 'dispute_snapshots is append-only: UPDATE is not allowed');
END;

CREATE TRIGGER IF NOT EXISTS trg_dispute_snapshots_no_delete
BEFORE DELETE ON dispute_snapshots
BEGIN
    SELECT RAISE(ABORT, 'dispute_snapshots is append-only: DELETE is not allowed');
END;

CREATE TRIGGER IF NOT EXISTS trg_refunds_no_update
BEFORE UPDATE ON refunds
BEGIN
    SELECT RAISE(ABORT, 'refunds is an immutable ledger: UPDATE is not allowed');
END;

CREATE TRIGGER IF NOT EXISTS trg_refunds_no_delete
BEFORE DELETE ON refunds
BEGIN
    SELECT RAISE(ABORT, 'refunds is an immutable ledger: DELETE is not allowed');
END;

-- Operator fare clearing. The first closing (seq 0) freezes the settlement
-- window and the per-station-pair operator rules; follow-up batches (seq >= 1)
-- reference it via root_clearing_id and inherit both frozen values. Both
-- tables are append-only: corrections to tap events never rewrite a closed
-- batch -- adjustment batches only append signed deltas to the entries table.
CREATE TABLE IF NOT EXISTS clearing_batches (
    clearing_id           TEXT PRIMARY KEY,
    root_clearing_id      TEXT NOT NULL REFERENCES clearing_batches(clearing_id),
    seq                   INTEGER NOT NULL,            -- 0 = first closing, 1+ = adjustments
    kind                  TEXT NOT NULL CHECK (kind IN ('initial', 'adjustment')),
    payload_hash          TEXT NOT NULL,              -- sha256 of the canonical request
    window_start          TEXT NOT NULL,              -- frozen on the first closing
    window_end            TEXT NOT NULL,              -- half-open window on tap-out time
    rules                 TEXT NOT NULL,               -- canonical JSON, frozen on the first closing
    base_clearing_id      TEXT,                       -- referenced first closing (adjustments)
    trip_count            INTEGER NOT NULL,            -- current in-window trips seen
    reversed_trip_count   INTEGER NOT NULL,            -- trips reversed to zero
    ledger_row_count      INTEGER NOT NULL,
    gross_amount_cents    INTEGER NOT NULL,            -- sum of current trip grosses
    net_amount_cents      INTEGER NOT NULL,            -- signed amount booked by this batch
    created_at            TEXT NOT NULL,
    UNIQUE (root_clearing_id, seq)
);

-- Immutable per-trip / per-operator ledger. A first closing inserts the full
-- split of each in-window trip (basis_points populated). Adjustment batches
-- append only signed difference rows (basis_points NULL): the current target
-- split minus the cumulative amount booked by earlier batches of the chain.
CREATE TABLE IF NOT EXISTS clearing_entries (
    clearing_id              TEXT NOT NULL REFERENCES clearing_batches(clearing_id),
    seq                      INTEGER NOT NULL,
    trip_id                  TEXT NOT NULL,
    passenger_id             TEXT NOT NULL,
    from_station             TEXT NOT NULL,
    to_station               TEXT NOT NULL,
    ended_at                 TEXT NOT NULL,
    operator_id              TEXT NOT NULL,
    basis_points             INTEGER,          -- share rule, NULL on adjustment rows
    gross_amount_cents       INTEGER NOT NULL, -- current trip gross at this batch (0 when reversed)
    amount_cents             INTEGER NOT NULL, -- initial split or signed adjustment delta
    prior_amount_cents       INTEGER NOT NULL,
    cumulative_amount_cents  INTEGER NOT NULL,
    PRIMARY KEY (clearing_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_clearing_entries_chain
    ON clearing_entries (trip_id, operator_id);

CREATE TRIGGER IF NOT EXISTS trg_clearing_batches_no_update
BEFORE UPDATE ON clearing_batches
BEGIN
    SELECT RAISE(ABORT, 'clearing_batches is append-only: UPDATE is not allowed');
END;

CREATE TRIGGER IF NOT EXISTS trg_clearing_batches_no_delete
BEFORE DELETE ON clearing_batches
BEGIN
    SELECT RAISE(ABORT, 'clearing_batches is append-only: DELETE is not allowed');
END;

CREATE TRIGGER IF NOT EXISTS trg_clearing_entries_no_update
BEFORE UPDATE ON clearing_entries
BEGIN
    SELECT RAISE(ABORT, 'clearing_entries is an immutable ledger: UPDATE is not allowed');
END;

CREATE TRIGGER IF NOT EXISTS trg_clearing_entries_no_delete
BEFORE DELETE ON clearing_entries
BEGIN
    SELECT RAISE(ABORT, 'clearing_entries is an immutable ledger: DELETE is not allowed');
END;

-- Tap-event quality scans. These tables are deliberately isolated from the
-- settlement domain: a scan only ever reads events and appends its own rows.
-- Events, trips, trip_charges, disputes and the audit_log are never touched by
-- a scan, and scan writes do not append audit records. A scan_id maps to at
-- most one request; the canonical payload hash enforces idempotency (same
-- payload replays the stored result, different payload -> 409).
CREATE TABLE IF NOT EXISTS event_quality_scans (
    scan_id                   TEXT PRIMARY KEY,
    payload_hash              TEXT NOT NULL,     -- sha256 of the canonical request
    window_start              TEXT NOT NULL,     -- ISO-8601, normalised to UTC
    window_end                TEXT NOT NULL,     -- half-open window on occurred_at
    passengers                TEXT,              -- JSON allow-list, NULL = all passengers
    duplicate_window_seconds  INTEGER NOT NULL,
    min_travel_seconds        TEXT NOT NULL,     -- JSON: {"OUT": {"IN": seconds}}
    event_count               INTEGER NOT NULL,  -- events scanned inside the window
    alert_count               INTEGER NOT NULL,
    created_at                TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS event_quality_alerts (
    scan_id                   TEXT NOT NULL REFERENCES event_quality_scans(scan_id),
    seq                       INTEGER NOT NULL,  -- 0-based, in scan order
    event_id                  TEXT NOT NULL,     -- the triggering (later) event
    passenger_id              TEXT NOT NULL,
    rule                      TEXT NOT NULL CHECK (rule IN
                                ('duplicate_tap', 'min_travel_violation')),
    actual_interval_seconds   REAL NOT NULL,     -- elapsed gap between evidence events
    threshold_seconds         INTEGER NOT NULL,  -- configured bound that was breached
    evidence                  TEXT NOT NULL,     -- JSON: ordered chain of tap events
    PRIMARY KEY (scan_id, seq)
);

CREATE TRIGGER IF NOT EXISTS trg_event_quality_scans_no_update
BEFORE UPDATE ON event_quality_scans
BEGIN
    SELECT RAISE(ABORT, 'event_quality_scans is append-only: UPDATE is not allowed');
END;

CREATE TRIGGER IF NOT EXISTS trg_event_quality_scans_no_delete
BEFORE DELETE ON event_quality_scans
BEGIN
    SELECT RAISE(ABORT, 'event_quality_scans is append-only: DELETE is not allowed');
END;

CREATE TRIGGER IF NOT EXISTS trg_event_quality_alerts_no_update
BEFORE UPDATE ON event_quality_alerts
BEGIN
    SELECT RAISE(ABORT, 'event_quality_alerts is append-only: UPDATE is not allowed');
END;

CREATE TRIGGER IF NOT EXISTS trg_event_quality_alerts_no_delete
BEFORE DELETE ON event_quality_alerts
BEGIN
    SELECT RAISE(ABORT, 'event_quality_alerts is append-only: DELETE is not allowed');
END;

-- Anonymous k-anonymous trajectory exports. Like the quality scans these
-- tables are deliberately isolated: an export only ever reads trips and
-- appends its own rows. Events, trips, trip_charges, disputes and the
-- audit_log are never touched, and export writes do not append audit records.
-- An export_id maps to at most one request; the canonical payload hash
-- enforces idempotency (same payload replays the stored result, different
-- payload -> 409). Only pseudonymised (HMAC) passenger identifiers are ever
-- stored here; raw passenger ids are never written.
CREATE TABLE IF NOT EXISTS privacy_exports (
    export_id                   TEXT PRIMARY KEY,
    payload_hash                TEXT NOT NULL,     -- sha256 of the canonical request
    window_start                TEXT NOT NULL,     -- ISO-8601, normalised to UTC
    window_end                  TEXT NOT NULL,     -- half-open window on started_at
    bucket_minutes              INTEGER NOT NULL,  -- epoch-anchored UTC buckets
    k                           INTEGER NOT NULL,  -- min distinct passengers per class
    station_groups              TEXT NOT NULL,     -- canonical JSON: {station: group}
    input_trip_count            INTEGER NOT NULL,
    equivalence_class_count     INTEGER NOT NULL,
    retained_class_count        INTEGER NOT NULL,
    suppressed_class_count      INTEGER NOT NULL,
    retained_trip_count         INTEGER NOT NULL,
    suppressed_trip_count       INTEGER NOT NULL,
    created_at                  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS privacy_export_trips (
    export_id        TEXT NOT NULL REFERENCES privacy_exports(export_id),
    seq              INTEGER NOT NULL,  -- 0-based, in export order
    pseudonym        TEXT NOT NULL,     -- HMAC-SHA256 of passenger_id
    bucket_start     TEXT NOT NULL,     -- start of the epoch-anchored UTC bucket
    origin_group     TEXT NOT NULL,
    destination_group TEXT NOT NULL,
    PRIMARY KEY (export_id, seq)
);

CREATE TRIGGER IF NOT EXISTS trg_privacy_exports_no_update
BEFORE UPDATE ON privacy_exports
BEGIN
    SELECT RAISE(ABORT, 'privacy_exports is append-only: UPDATE is not allowed');
END;

CREATE TRIGGER IF NOT EXISTS trg_privacy_exports_no_delete
BEFORE DELETE ON privacy_exports
BEGIN
    SELECT RAISE(ABORT, 'privacy_exports is append-only: DELETE is not allowed');
END;

CREATE TRIGGER IF NOT EXISTS trg_privacy_export_trips_no_update
BEFORE UPDATE ON privacy_export_trips
BEGIN
    SELECT RAISE(ABORT, 'privacy_export_trips is append-only: UPDATE is not allowed');
END;

CREATE TRIGGER IF NOT EXISTS trg_privacy_export_trips_no_delete
BEFORE DELETE ON privacy_export_trips
BEGIN
    SELECT RAISE(ABORT, 'privacy_export_trips is append-only: DELETE is not allowed');
END;

-- Sudden-disruption evacuation replans. Like the quality scans and privacy
-- exports these tables are deliberately isolated: a replan computes purely
-- from the request payload and appends its own frozen result. Events, trips,
-- charges, disputes and the audit_log are never touched, and replan writes do
-- not append audit records. A replan_id maps to at most one request; the
-- canonical payload hash enforces idempotency (same payload replays the stored
-- result, different payload -> 409).
CREATE TABLE IF NOT EXISTS disruption_replans (
    replan_id              TEXT PRIMARY KEY,
    payload_hash           TEXT NOT NULL,     -- sha256 of the canonical request
    payload                TEXT NOT NULL,     -- canonical JSON of the normalised request
    min_transfer_seconds   INTEGER NOT NULL,
    group_count            INTEGER NOT NULL,
    assigned_count         INTEGER NOT NULL,
    stranded_count         INTEGER NOT NULL,
    result                 TEXT NOT NULL,     -- JSON: the exact API response
    created_at             TEXT NOT NULL
);

CREATE TRIGGER IF NOT EXISTS trg_disruption_replans_no_update
BEFORE UPDATE ON disruption_replans
BEGIN
    SELECT RAISE(ABORT, 'disruption_replans is append-only: UPDATE is not allowed');
END;

CREATE TRIGGER IF NOT EXISTS trg_disruption_replans_no_delete
BEFORE DELETE ON disruption_replans
BEGIN
    SELECT RAISE(ABORT, 'disruption_replans is append-only: DELETE is not allowed');
END;

-- Append-only, hash-chained business audit log. One global sequence spans all
-- operation types; record_n hashes seq | operation | timestamp | canonical
-- request/result summaries | record_{n-1}'s hash. Rows are only ever inserted
-- inside the same transaction as the business write they describe.
CREATE TABLE IF NOT EXISTS audit_log (
    seq              INTEGER PRIMARY KEY,   -- global monotonic sequence, starts at 1
    operation        TEXT NOT NULL CHECK (operation IN
                         ('event_processed', 'event_corrected',
                          'recon_batch_created', 'network_validated',
                          'network_snapshot_created',
                          'dispute_opened', 'dispute_resolved',
                          'clearing_closed',
                          'event_held', 'event_hold_released')),
    occurred_at      TEXT NOT NULL,         -- ISO-8601, normalised to UTC
    request_summary  TEXT NOT NULL,         -- canonical JSON of the normalised request
    result_summary   TEXT NOT NULL,         -- canonical JSON of the normalised result
    prev_hash        TEXT NOT NULL,         -- previous record_hash, '' for the genesis record
    record_hash      TEXT NOT NULL          -- sha256 over this record's canonical content
);

CREATE TRIGGER IF NOT EXISTS trg_audit_no_update
BEFORE UPDATE ON audit_log
BEGIN
    SELECT RAISE(ABORT, 'audit_log is append-only: UPDATE is not allowed');
END;

CREATE TRIGGER IF NOT EXISTS trg_audit_no_delete
BEFORE DELETE ON audit_log
BEGIN
    SELECT RAISE(ABORT, 'audit_log is append-only: DELETE is not allowed');
END;
"""

# Rebuild statement used when upgrading a database created before a newer
# operation type (network validation, then fare disputes): SQLite cannot ALTER
# a CHECK constraint, so the audit table is rebuilt with the rows preserved
# (its hash chain lives entirely in columns).
_AUDIT_OPERATIONS = (
    "event_processed", "event_corrected", "recon_batch_created",
    "network_validated", "network_snapshot_created",
    "dispute_opened", "dispute_resolved", "clearing_closed",
    "event_held", "event_hold_released",
)

_AUDIT_UPGRADE = """
DROP TRIGGER IF EXISTS trg_audit_no_update;
DROP TRIGGER IF EXISTS trg_audit_no_delete;
ALTER TABLE audit_log RENAME TO audit_log_legacy;
CREATE TABLE audit_log (
    seq              INTEGER PRIMARY KEY,
    operation        TEXT NOT NULL CHECK (operation IN
                         ('event_processed', 'event_corrected',
                          'recon_batch_created', 'network_validated',
                          'network_snapshot_created',
                          'dispute_opened', 'dispute_resolved',
                          'clearing_closed',
                          'event_held', 'event_hold_released')),
    occurred_at      TEXT NOT NULL,
    request_summary  TEXT NOT NULL,
    result_summary   TEXT NOT NULL,
    prev_hash        TEXT NOT NULL,
    record_hash      TEXT NOT NULL
);
INSERT INTO audit_log (seq, operation, occurred_at, request_summary,
                       result_summary, prev_hash, record_hash)
SELECT seq, operation, occurred_at, request_summary, result_summary,
       prev_hash, record_hash FROM audit_log_legacy;
DROP TABLE audit_log_legacy;
CREATE TRIGGER trg_audit_no_update
BEFORE UPDATE ON audit_log
BEGIN
    SELECT RAISE(ABORT, 'audit_log is append-only: UPDATE is not allowed');
END;
CREATE TRIGGER trg_audit_no_delete
BEFORE DELETE ON audit_log
BEGIN
    SELECT RAISE(ABORT, 'audit_log is append-only: DELETE is not allowed');
END;
"""


class Database:
    def __init__(self, path: str):
        self.path = path
        self._lock = threading.RLock()
        if path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        with self._conn:
            self._conn.executescript(SCHEMA)
            # Upgrade an older database whose audit_log CHECK predates one of
            # the current operation types (network validation, disputes).
            sql = self._conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='audit_log'"
            ).fetchone()[0]
            if any(op not in sql for op in _AUDIT_OPERATIONS):
                self._conn.executescript(_AUDIT_UPGRADE)
            # Upgrade an older database whose events table predates the
            # tap-event review hold marker (the events table has no append-only
            # triggers, so a plain additive ALTER is enough).
            event_cols = {
                r[1] for r in self._conn.execute("PRAGMA table_info(events)")
            }
            if "held_id" not in event_cols:
                self._conn.execute(
                    "ALTER TABLE events ADD COLUMN held_id TEXT"
                )
            # Unconditional: a fresh DB gets it here too (the CREATE TABLE
            # above cannot include this index, since SCHEMA must also run on
            # pre-hold databases before the additive ALTER above).
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_events_held ON events (held_id)"
            )

    def lock(self) -> threading.RLock:
        return self._lock

    @property
    def conn(self) -> sqlite3.Connection:
        return self._conn

    def close(self) -> None:
        self._conn.close()

    def online_backup(
        self,
        dst_path: str,
        *,
        pages: int = 512,
        progress=None,
    ) -> None:
        """Write a consistent snapshot of the live database to ``dst_path``.

        Uses the SQLite online backup API via a *separate* source connection,
        never a file copy: the backup pages through the database while readers
        and writers (tap submissions) keep running, restarting transparently
        whenever the source is modified mid-copy. The application lock is
        deliberately not held, so ``POST /events`` is not blocked.

        ``progress`` receives ``(status, remaining, page_count)`` after each
        batch, exactly like sqlite3's backup callback (``status`` is 1 when
        the copy restarted due to concurrent writes, 0 otherwise), and may
        sleep to throttle.
        """
        if self.path == ":memory:":
            # An in-memory database has no shared backing file; another
            # connection would see an empty database. Fall back to a
            # consistent copy taken under the lock (tests only).
            dest = sqlite3.connect(dst_path)
            try:
                with self._lock:
                    self._conn.backup(dest, pages=pages, progress=progress)
            finally:
                dest.close()
            return

        os.makedirs(os.path.dirname(os.path.abspath(dst_path)), exist_ok=True)
        # Independent source connection: the backup neither shares the
        # application connection nor holds its lock. A read-only URI
        # connection guarantees the copy never disturbs the live database.
        uri = "file:" + self.path.replace("?", "%3f").replace("#", "%23")
        source = sqlite3.connect(f"{uri}?mode=ro", uri=True, timeout=30)
        dest = sqlite3.connect(dst_path, timeout=30)
        try:
            source.execute("PRAGMA busy_timeout=30000")
            source.backup(dest, pages=pages, progress=progress)
        finally:
            dest.close()
            source.close()
