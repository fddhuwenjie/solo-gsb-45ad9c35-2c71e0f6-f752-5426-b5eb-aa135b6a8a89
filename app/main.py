"""Public transit fare settlement API."""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from starlette.background import BackgroundTask
from starlette.responses import FileResponse

from .audit import append_audit, canonical_json, verify_chain
from .clearing import (
    canonical_adjustment_payload,
    canonical_initial_payload,
    canonical_rules,
    last_seen_trips,
    load_clearing_trips,
    payload_hash as clearing_payload_hash,
    prior_operator_totals,
    rule_map_from_canonical,
    split_amount,
    validate_rules as validate_clearing_rules,
)
from .config import FareConfig, load_config
from .db import Database
from .disputes import (
    canonical_dispute_payload,
    canonical_resolve_payload,
    dispute_response,
    payload_hash,
    refund_id_for,
)
from .event_holds import (
    build_hold_view,
    canonical_hold_payload,
    canonical_release_payload,
    event_payload_hash,
    hold_response,
    payload_hash as hold_payload_hash,
    referenced_event_ids,
    release_id_for,
)
from .disruption import (
    build_replan,
    canonical_replan_payload,
    normalise_input,
    payload_hash as replan_payload_hash,
)
from .event_quality import (
    canonical_scan_payload,
    load_window_events,
    payload_hash as scan_payload_hash,
    scan_events,
)
from .models import (
    Adjustment,
    AuditEntry,
    AuditList,
    AuditVerification,
    Bill,
    ClearingAdjustmentIn,
    ClearingBatchOut,
    ClearingEntryOut,
    ClearingIn,
    ClearingOperatorTotal,
    DisputeIn,
    DisputeOut,
    DisputeResolveIn,
    DisruptionReplanIn,
    DisruptionReplanOut,
    EventHoldIn,
    EventHoldOut,
    EventHoldReleaseIn,
    EventQualityScanIn,
    EventQualityScanOut,
    EventResult,
    NetworkSnapshotIn,
    NetworkSnapshotOut,
    NetworkValidateIn,
    NetworkValidateOut,
    PendingEvent,
    PeriodTotal,
    PrivacyEquivalenceClass,
    PrivacyExportIn,
    PrivacyExportOut,
    PrivacyExportTrip,
    QualityAlert,
    QualityEvidenceEvent,
    ReconBatchIn,
    ReconBatchOut,
    ReconResultItem,
    ReconSummary,
    TapEventIn,
    TripOut,
)
from .network import NetEdge, validate_network
from .privacy import (
    UnmappedStationsError,
    build_export,
    canonical_export_payload,
    classes_from_trip_rows,
    load_station_groups,
    load_window_trips,
    payload_hash as export_payload_hash,
    stored_groups_json,
)
from .reconcile import CATEGORIES, ExternalRecord, SnapshotTrip, reconcile
from .settlement import iso_str, normalize_ts, settle_passenger

DB_PATH = os.environ.get("DB_PATH", "data/transit.db")

# Server-side secret for stable HMAC passenger pseudonymisation in privacy
# exports. It must come from configuration (never from the request) and stay
# stable across restarts: changing it rotates every pseudonym. Missing/empty
# configuration fails startup rather than silently weakening anonymity.
PRIVACY_SALT = os.environ.get("PRIVACY_SALT", "").strip()

# Paging parameters for the online backup. The defaults copy in reasonably
# sized batches without throttling; tests override these to stretch the copy
# and prove concurrent writes keep landing while a backup is running.
BACKUP_PAGES_PER_BATCH = 512
BACKUP_PROGRESS_CALLBACK = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not PRIVACY_SALT:
        raise RuntimeError(
            "PRIVACY_SALT is not configured: set it to a long, server-side"
            " secret string (see README). It keeps passenger pseudonyms"
            " stable; changing it rotates every pseudonym."
        )
    app.state.config = load_config()
    app.state.privacy_salt = PRIVACY_SALT
    app.state.db = Database(DB_PATH)
    yield
    app.state.db.close()


app = FastAPI(
    title="Transit Fare Settlement API",
    version="1.10.0",
    lifespan=lifespan,
)


def get_config(request: Request) -> FareConfig:
    return request.app.state.config


def get_db(request: Request) -> Database:
    return request.app.state.db


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/config/fares")
def fare_config(cfg: FareConfig = Depends(get_config)) -> dict:
    return {
        "currency": cfg.currency,
        "transfer_window_minutes": cfg.transfer_window_minutes,
        "transfer_discount_cents": cfg.transfer_discount_cents,
        "daily_cap_cents": cfg.daily_cap_cents,
        "weekly_cap_cents": cfg.weekly_cap_cents,
        "stations": cfg.stations,
        "zone_fares": cfg.zone_fares,
    }


@app.post("/events", response_model=EventResult, status_code=200)
def submit_event(
    body: TapEventIn,
    cfg: FareConfig = Depends(get_config),
    db: Database = Depends(get_db),
) -> EventResult:
    if cfg.zone_of(body.station_id) is None:
        raise HTTPException(
            status_code=422,
            detail=f"unknown station_id '{body.station_id}'; see GET /config/fares",
        )
    occurred = normalize_ts(body.occurred_at)
    occurred_s = iso_str(occurred)

    with db.lock(), db.conn:
        existing = db.conn.execute(
            "SELECT * FROM events WHERE event_id = ?", (body.event_id,)
        ).fetchone()

        if existing is not None:
            identical = (
                existing["passenger_id"] == body.passenger_id
                and existing["station_id"] == body.station_id
                and existing["tap_type"] == body.tap_type
                and existing["occurred_at"] == occurred_s
            )
            if identical:
                return EventResult(
                    event_id=body.event_id,
                    status="duplicate",
                    event_pairing=existing["status"],
                    pending_reason=existing["pending_reason"],
                    adjustments=[],
                )
            # A different payload is a correction -- unless the event's
            # complete payload is frozen under an open review hold. Held
            # events can never be corrected; the review must finish (release
            # or a new event id) first.
            if existing["held_id"] is not None:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"event_id '{body.event_id}' is held under review hold"
                        f" '{existing['held_id']}'; corrections are refused until"
                        " that hold is released"
                    ),
                )
            # Correction: same event id, different payload -> update and re-settle.
            db.conn.execute(
                "UPDATE events SET passenger_id = ?, station_id = ?, tap_type = ?,"
                " occurred_at = ? WHERE event_id = ?",
                (body.passenger_id, body.station_id, body.tap_type, occurred_s, body.event_id),
            )
            status = "corrected"
            affected = {existing["passenger_id"], body.passenger_id}
        else:
            seq = db.conn.execute("SELECT COALESCE(MAX(seq), 0) + 1 FROM events").fetchone()[0]
            db.conn.execute(
                "INSERT INTO events (event_id, passenger_id, station_id, tap_type,"
                " occurred_at, seq) VALUES (?,?,?,?,?,?)",
                (body.event_id, body.passenger_id, body.station_id, body.tap_type, occurred_s, seq),
            )
            status = "processed"
            affected = {body.passenger_id}

        adjustments: list[dict] = []
        for pid in affected:
            adjustments.extend(settle_passenger(db.conn, cfg, pid))

        row = db.conn.execute(
            "SELECT status, pending_reason FROM events WHERE event_id = ?", (body.event_id,)
        ).fetchone()

        result = EventResult(
            event_id=body.event_id,
            status=status,
            event_pairing=row["status"],
            pending_reason=row["pending_reason"],
            adjustments=[Adjustment(**a) for a in adjustments],
        )
        # Audit inside the same transaction: if the business write rolled
        # back, this record must not exist either.
        append_audit(
            db.conn,
            "event_processed" if status == "processed" else "event_corrected",
            {
                "event_id": body.event_id,
                "passenger_id": body.passenger_id,
                "station_id": body.station_id,
                "tap_type": body.tap_type,
                "occurred_at": occurred_s,
            },
            result.model_dump(),
        )

    return result


@app.get("/passengers/{passenger_id}/bill", response_model=Bill)
def passenger_bill(
    passenger_id: str,
    day: str | None = Query(default=None, description="UTC day, YYYY-MM-DD"),
    week: str | None = Query(default=None, description="ISO week, e.g. 2026-W39"),
    cfg: FareConfig = Depends(get_config),
    db: Database = Depends(get_db),
) -> Bill:
    sql = (
        "SELECT t.*, c.base_fare_cents, c.transfer_discount_cents, c.amount_cents, c.basis"
        " FROM trips t JOIN trip_charges c ON c.trip_id = t.trip_id"
        " WHERE t.passenger_id = ?"
    )
    params: list = [passenger_id]
    if day:
        sql += " AND t.day_key = ?"
        params.append(day)
    if week:
        sql += " AND t.week_key = ?"
        params.append(week)
    sql += " ORDER BY t.started_at, t.trip_id"

    with db.lock():
        rows = db.conn.execute(sql, params).fetchall()

    trips = [
        TripOut(
            trip_id=r["trip_id"],
            from_station=r["from_station"],
            to_station=r["to_station"],
            from_zone=r["from_zone"],
            to_zone=r["to_zone"],
            started_at=r["started_at"],
            ended_at=r["ended_at"],
            day_key=r["day_key"],
            week_key=r["week_key"],
            base_fare_cents=r["base_fare_cents"],
            transfer_discount_cents=r["transfer_discount_cents"],
            amount_cents=r["amount_cents"],
            basis=json.loads(r["basis"]),
        )
        for r in rows
    ]

    daily: dict[str, dict] = {}
    weekly: dict[str, dict] = {}
    for t in trips:
        d = daily.setdefault(t.day_key, {"amount": 0, "count": 0})
        d["amount"] += t.amount_cents
        d["count"] += 1
        w = weekly.setdefault(t.week_key, {"amount": 0, "count": 0})
        w["amount"] += t.amount_cents
        w["count"] += 1

    return Bill(
        passenger_id=passenger_id,
        currency=cfg.currency,
        trips=trips,
        daily_totals=[
            PeriodTotal(key=k, amount_cents=v["amount"], cap_cents=cfg.daily_cap_cents, trip_count=v["count"])
            for k, v in sorted(daily.items())
        ],
        weekly_totals=[
            PeriodTotal(key=k, amount_cents=v["amount"], cap_cents=cfg.weekly_cap_cents, trip_count=v["count"])
            for k, v in sorted(weekly.items())
        ],
        total_cents=sum(t.amount_cents for t in trips),
    )


@app.get("/pending", response_model=list[PendingEvent])
def pending_events(
    passenger_id: str | None = Query(default=None),
    db: Database = Depends(get_db),
) -> list[PendingEvent]:
    sql = "SELECT * FROM events WHERE status = 'pending' AND held_id IS NULL"
    params: list = []
    if passenger_id:
        sql += " AND passenger_id = ?"
        params.append(passenger_id)
    sql += " ORDER BY occurred_at, seq"
    with db.lock():
        rows = db.conn.execute(sql, params).fetchall()
    return [
        PendingEvent(
            event_id=r["event_id"],
            passenger_id=r["passenger_id"],
            station_id=r["station_id"],
            tap_type=r["tap_type"],
            occurred_at=r["occurred_at"],
            reason=r["pending_reason"],
        )
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Tap-event review holds (刷卡事件复核暂存与释放)
# ---------------------------------------------------------------------------


def _load_hold(db: Database, hold_id: str, status: str) -> EventHoldOut:
    hold = db.conn.execute(
        "SELECT * FROM event_holds WHERE hold_id = ?", (hold_id,)
    ).fetchone()
    if hold is None:
        raise HTTPException(
            status_code=404, detail=f"event review hold '{hold_id}' not found"
        )
    return hold_response(db.conn, hold, status)


@app.post("/events/review-holds", response_model=EventHoldOut, status_code=200)
def create_event_hold(
    body: EventHoldIn,
    cfg: FareConfig = Depends(get_config),
    db: Database = Depends(get_db),
) -> EventHoldOut:
    """Freeze one or more of a passenger's events for review. One transaction
    verifies ownership, blocks holds over events already booked by a
    reconciliation/clearing/resolved dispute, snapshots every complete event
    payload, marks the events held and re-settles the passenger without them."""
    if len(set(body.event_ids)) != len(body.event_ids):
        raise HTTPException(status_code=422, detail="duplicate event_id in event_ids")

    canonical_payload = canonical_hold_payload(body)
    p_hash = hold_payload_hash(canonical_payload)
    event_ids = canonical_payload["event_ids"]

    with db.lock(), db.conn:
        existing = db.conn.execute(
            "SELECT payload_hash FROM event_holds WHERE hold_id = ?",
            (body.hold_id,),
        ).fetchone()
        if existing is not None:
            if existing["payload_hash"] != p_hash:
                raise HTTPException(
                    status_code=409,
                    detail=f"hold_id '{body.hold_id}' already exists with a different payload",
                )
            # Idempotent replay: the frozen hold/snapshots are never recomputed.
            return _load_hold(db, body.hold_id, "duplicate")

        rows = db.conn.execute(
            "SELECT * FROM events WHERE event_id IN ({})".format(
                ",".join("?" for _ in event_ids)
            ),
            event_ids,
        ).fetchall()
        by_id = {r["event_id"]: r for r in rows}
        missing = sorted(set(event_ids) - set(by_id))
        if missing:
            raise HTTPException(
                status_code=422,
                detail=f"event_id(s) not found: {missing}",
            )
        foreign = sorted(
            eid for eid, r in by_id.items()
            if r["passenger_id"] != body.passenger_id
        )
        if foreign:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"event_id(s) {foreign} do not belong to passenger"
                    f" '{body.passenger_id}'"
                ),
            )
        already_held = sorted(
            eid for eid, r in by_id.items() if r["held_id"] is not None
        )
        if already_held:
            holders = sorted({by_id[e]["held_id"] for e in already_held})
            raise HTTPException(
                status_code=409,
                detail=(
                    f"event_id(s) {already_held} are already held under"
                    f" review hold(s) {holders}"
                ),
            )
        referenced = referenced_event_ids(db.conn, event_ids)
        if referenced:
            # Whole-batch refusal: no marker or snapshot row is written.
            raise HTTPException(
                status_code=409,
                detail=(
                    f"event_id(s) {referenced} are already referenced by a"
                    " reconciliation batch, a clearing batch or a resolved"
                    " dispute and cannot be held"
                ),
            )

        frozen_at = iso_str(datetime.now(timezone.utc))
        # Mark the events held first; settle_passenger then sees none of them,
        # so pairing and billing stop using the whole frozen set atomically.
        db.conn.executemany(
            "UPDATE events SET held_id = ? WHERE event_id = ?",
            [(body.hold_id, eid) for eid in event_ids],
        )
        adjustments = settle_passenger(db.conn, cfg, body.passenger_id)

        snapshot_specs = [
            {
                "seq": seq,
                "event_id": r["event_id"],
                "passenger_id": r["passenger_id"],
                "station_id": r["station_id"],
                "tap_type": r["tap_type"],
                "occurred_at": r["occurred_at"],
                "event_seq": r["seq"],
                "payload_hash": event_payload_hash(r),
                "frozen_at": frozen_at,
            }
            for seq, r in enumerate((by_id[e] for e in event_ids))
        ]
        # Build the frozen response before inserting the hold row, so the
        # immutable row stores the complete hold-time result in one INSERT.
        response = build_hold_view(
            hold_id=body.hold_id,
            passenger_id=body.passenger_id,
            review_reason=body.review_reason,
            event_ids=event_ids,
            created_at=frozen_at,
            snapshot_rows=snapshot_specs,
            hold_adjustments=[Adjustment(**a) for a in adjustments],
        )
        db.conn.execute(
            "INSERT INTO event_holds (hold_id, payload_hash, payload,"
            " passenger_id, review_reason, event_count, result, created_at)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (
                body.hold_id,
                p_hash,
                canonical_json(canonical_payload),
                body.passenger_id,
                body.review_reason,
                len(event_ids),
                canonical_json(response.model_dump()),
                frozen_at,
            ),
        )
        db.conn.executemany(
            "INSERT INTO event_hold_snapshots (hold_id, seq, event_id,"
            " passenger_id, station_id, tap_type, occurred_at, event_seq,"
            " payload_hash, frozen_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
            [
                (
                    body.hold_id,
                    s["seq"],
                    s["event_id"],
                    s["passenger_id"],
                    s["station_id"],
                    s["tap_type"],
                    s["occurred_at"],
                    s["event_seq"],
                    s["payload_hash"],
                    s["frozen_at"],
                )
                for s in snapshot_specs
            ],
        )
        append_audit(
            db.conn,
            "event_held",
            canonical_payload,
            response.model_dump(),
        )

    return response


@app.post(
    "/events/review-holds/{hold_id}/release",
    response_model=EventHoldOut,
    status_code=200,
)
def release_event_hold(
    hold_id: str,
    body: EventHoldReleaseIn,
    cfg: FareConfig = Depends(get_config),
    db: Database = Depends(get_db),
) -> EventHoldOut:
    """Release a review hold after the expected optimistic version. The frozen
    per-event payload hashes must still match the live rows; on a mismatch (or
    a version conflict) the release aborts and every event stays held. On
    success the events return to settlement in their original (occurred_at,
    seq) order and the signed trip deltas are appended as an immutable release
    result."""
    canonical_payload = canonical_release_payload(body, hold_id)
    p_hash = hold_payload_hash(canonical_payload)

    with db.lock(), db.conn:
        hold = db.conn.execute(
            "SELECT * FROM event_holds WHERE hold_id = ?", (hold_id,)
        ).fetchone()
        if hold is None:
            raise HTTPException(
                status_code=404, detail=f"event review hold '{hold_id}' not found"
            )

        existing_release = db.conn.execute(
            "SELECT * FROM event_hold_releases WHERE hold_id = ?", (hold_id,)
        ).fetchone()
        if existing_release is not None:
            if existing_release["payload_hash"] != p_hash:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"hold '{hold_id}' is already released (version 2); the"
                        " immutable release result cannot be re-released"
                    ),
                )
            # Idempotent replay of the identical release.
            return _load_hold(db, hold_id, "duplicate")

        if body.expected_version != 1:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"version conflict: hold '{hold_id}' is at version 1"
                    f" (held); expected_version {body.expected_version} is stale"
                ),
            )

        snapshots = db.conn.execute(
            "SELECT * FROM event_hold_snapshots WHERE hold_id = ? ORDER BY seq",
            (hold_id,),
        ).fetchall()
        live_rows = {
            r["event_id"]: r
            for r in db.conn.execute(
                "SELECT * FROM events WHERE event_id IN ({})".format(
                    ",".join("?" for _ in snapshots)
                ),
                [s["event_id"] for s in snapshots],
            )
        }
        diverged: list[str] = []
        for snap in snapshots:
            live = live_rows.get(snap["event_id"])
            if (
                live is None
                or live["held_id"] != hold_id
                or event_payload_hash(live) != snap["payload_hash"]
            ):
                diverged.append(snap["event_id"])
        if diverged:
            # Snapshot divergence refuses the release and leaves the hold in
            # place: the frozen payloads are the review's source of truth.
            raise HTTPException(
                status_code=409,
                detail=(
                    f"event snapshot mismatch for event_id(s) {sorted(diverged)};"
                    " release refused and the events remain held"
                ),
            )

        released_at = iso_str(datetime.now(timezone.utc))
        # Clear the hold markers and re-settle. The events' original arrival
        # seq was never touched, so pairing resumes in the original
        # (occurred_at, seq) order.
        db.conn.executemany(
            "UPDATE events SET held_id = NULL WHERE event_id = ?",
            [(snap["event_id"],) for snap in snapshots],
        )
        adjustments = settle_passenger(db.conn, cfg, hold["passenger_id"])

        release_result = {
            "release_id": release_id_for(hold_id),
            "expected_version": body.expected_version,
            "released_at": released_at,
            "adjustments": [Adjustment(**a).model_dump() for a in adjustments],
        }
        db.conn.execute(
            "INSERT INTO event_hold_releases (release_id, hold_id, payload_hash,"
            " expected_version, result, created_at) VALUES (?,?,?,?,?,?)",
            (
                release_id_for(hold_id),
                hold_id,
                p_hash,
                body.expected_version,
                canonical_json(release_result),
                released_at,
            ),
        )
        response = _load_hold(db, hold_id, "created")
        append_audit(
            db.conn,
            "event_hold_released",
            canonical_payload,
            response.model_dump(),
        )

    return response


@app.get("/events/review-holds/{hold_id}", response_model=EventHoldOut)
def get_event_hold(
    hold_id: str,
    db: Database = Depends(get_db),
) -> EventHoldOut:
    with db.lock():
        return _load_hold(db, hold_id, "created")


def _recon_response(meta: dict, items: list[dict], status: str, currency: str) -> ReconBatchOut:
    counts = {c: 0 for c in CATEGORIES}
    for it in items:
        counts[it["category"]] += 1
    return ReconBatchOut(
        batch_id=meta["batch_id"],
        status=status,
        window_start=meta["window_start"],
        window_end=meta["window_end"],
        tolerance_seconds=meta["tolerance_seconds"],
        currency=currency,
        summary=ReconSummary(
            **counts,
            snapshot_trip_count=meta["snapshot_trip_count"],
            external_record_count=meta["external_record_count"],
        ),
        results=[ReconResultItem(**it) for it in items],
    )


def _load_recon_batch(db: Database, batch_id: str) -> tuple[dict, list[dict]]:
    b = db.conn.execute(
        "SELECT * FROM recon_batches WHERE batch_id = ?", (batch_id,)
    ).fetchone()
    if b is None:
        raise HTTPException(
            status_code=404, detail=f"reconciliation batch '{batch_id}' not found"
        )
    rows = db.conn.execute(
        "SELECT category, passenger_id, trip_id, serial_no, internal_amount_cents,"
        " external_amount_cents, difference_cents, internal_ended_at,"
        " external_charged_at, match_method, reason"
        " FROM recon_results WHERE batch_id = ? ORDER BY seq",
        (batch_id,),
    ).fetchall()
    meta = {
        "batch_id": b["batch_id"],
        "window_start": b["window_start"],
        "window_end": b["window_end"],
        "tolerance_seconds": b["tolerance_seconds"],
        "snapshot_trip_count": b["snapshot_trip_count"],
        "external_record_count": b["external_record_count"],
    }
    return meta, [dict(r) for r in rows]


@app.post("/reconciliations", response_model=ReconBatchOut, status_code=200)
def create_reconciliation(
    body: ReconBatchIn,
    cfg: FareConfig = Depends(get_config),
    db: Database = Depends(get_db),
) -> ReconBatchOut:
    window_start = normalize_ts(body.window_start)
    window_end = normalize_ts(body.window_end)
    if window_end <= window_start:
        raise HTTPException(status_code=422, detail="window_end must be after window_start")
    serials = [r.serial_no for r in body.records]
    if len(set(serials)) != len(serials):
        raise HTTPException(status_code=422, detail="duplicate serial_no in records")

    records = [
        ExternalRecord(
            serial_no=r.serial_no,
            passenger_id=r.passenger_id,
            charged_at=normalize_ts(r.charged_at),
            amount_cents=r.amount_cents,
            trip_id=r.trip_id,
        )
        for r in body.records
    ]
    # Canonical payload: timestamps normalised, records sorted by serial_no,
    # so semantically identical submissions hash identically regardless of
    # record order or offset notation.
    canonical_payload = {
        "batch_id": body.batch_id,
        "window_start": iso_str(window_start),
        "window_end": iso_str(window_end),
        "tolerance_seconds": body.tolerance_seconds,
        "records": sorted(
            (
                {
                    "serial_no": r.serial_no,
                    "passenger_id": r.passenger_id,
                    "charged_at": iso_str(r.charged_at),
                    "amount_cents": r.amount_cents,
                    "trip_id": r.trip_id,
                }
                for r in records
            ),
            key=lambda d: d["serial_no"],
        ),
    }
    payload_hash = hashlib.sha256(
        canonical_json(canonical_payload).encode("utf-8")
    ).hexdigest()

    with db.lock(), db.conn:
        existing = db.conn.execute(
            "SELECT payload_hash FROM recon_batches WHERE batch_id = ?", (body.batch_id,)
        ).fetchone()
        if existing is not None:
            if existing["payload_hash"] != payload_hash:
                raise HTTPException(
                    status_code=409,
                    detail=f"batch_id '{body.batch_id}' already exists with a different payload",
                )
            meta, items = _load_recon_batch(db, body.batch_id)
            return _recon_response(meta, items, "duplicate", cfg.currency)

        # Snapshot: internal trip charges whose tap-out time falls inside the
        # half-open window. Filtered in Python because stored ISO strings may
        # carry optional fractional seconds, which breaks lexicographic
        # comparison at the boundary.
        rows = db.conn.execute(
            "SELECT t.trip_id, t.passenger_id, t.ended_at, c.amount_cents"
            " FROM trips t JOIN trip_charges c ON c.trip_id = t.trip_id"
        ).fetchall()
        snapshot = []
        for r in rows:
            ended = normalize_ts(datetime.fromisoformat(r["ended_at"].replace("Z", "+00:00")))
            if window_start <= ended < window_end:
                snapshot.append(
                    SnapshotTrip(
                        trip_id=r["trip_id"],
                        passenger_id=r["passenger_id"],
                        ended_at=ended,
                        amount_cents=r["amount_cents"],
                    )
                )
        snapshot.sort(key=lambda t: (t.ended_at, t.trip_id))

        items = reconcile(snapshot, records, body.tolerance_seconds)
        meta = {
            "batch_id": body.batch_id,
            "window_start": iso_str(window_start),
            "window_end": iso_str(window_end),
            "tolerance_seconds": body.tolerance_seconds,
            "snapshot_trip_count": len(snapshot),
            "external_record_count": len(records),
        }
        db.conn.execute(
            "INSERT INTO recon_batches (batch_id, payload_hash, window_start, window_end,"
            " tolerance_seconds, snapshot_trip_count, external_record_count, created_at)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (
                body.batch_id,
                payload_hash,
                meta["window_start"],
                meta["window_end"],
                body.tolerance_seconds,
                len(snapshot),
                len(records),
                iso_str(datetime.now(timezone.utc)),
            ),
        )
        db.conn.executemany(
            "INSERT INTO recon_results (batch_id, seq, category, passenger_id, trip_id,"
            " serial_no, internal_amount_cents, external_amount_cents, difference_cents,"
            " internal_ended_at, external_charged_at, match_method, reason)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                (
                    body.batch_id,
                    seq,
                    it["category"],
                    it["passenger_id"],
                    it["trip_id"],
                    it["serial_no"],
                    it["internal_amount_cents"],
                    it["external_amount_cents"],
                    it["difference_cents"],
                    it["internal_ended_at"],
                    it["external_charged_at"],
                    it["match_method"],
                    it["reason"],
                )
                for seq, it in enumerate(items)
            ],
        )

        response = _recon_response(meta, items, "created", cfg.currency)
        # Audit inside the same transaction as the batch insert: a failed
        # business write rolls the audit record back as well.
        append_audit(
            db.conn,
            "recon_batch_created",
            canonical_payload,
            response.model_dump(),
        )

    return response


# ---------------------------------------------------------------------------
# Transfer network validation
# ---------------------------------------------------------------------------


def _canonical_network_payload(body: NetworkValidateIn, start, end) -> dict:
    """Canonical request: timestamps normalised to UTC, station/hub lists and
    edge lists sorted, so semantically identical submissions hash identically
    regardless of array order or offset notation."""
    return {
        "validation_id": body.validation_id,
        "effective_start": iso_str(start),
        "effective_end": iso_str(end),
        "hubs": sorted(body.hubs),
        "stations": sorted(body.stations),
        "segments": sorted(
            (
                {
                    "line_id": s.line_id,
                    "segment_id": s.segment_id,
                    "from_station": s.from_station,
                    "to_station": s.to_station,
                    "start_at": iso_str(normalize_ts(s.start_at)),
                    "end_at": iso_str(normalize_ts(s.end_at)),
                }
                for s in body.segments
            ),
            key=lambda d: (
                d["line_id"], d["segment_id"], d["from_station"], d["to_station"],
                d["start_at"], d["end_at"],
            ),
        ),
        "transfers": sorted(
            (
                {
                    "from_station": t.from_station,
                    "to_station": t.to_station,
                    "start_at": iso_str(normalize_ts(t.start_at)),
                    "end_at": iso_str(normalize_ts(t.end_at)),
                }
                for t in body.transfers
            ),
            key=lambda d: (d["from_station"], d["to_station"], d["start_at"], d["end_at"]),
        ),
    }


def _validate_network_input(body: NetworkValidateIn, start, end):
    """Structural checks that make validation meaningless; returns an HTTP 422
    detail string, or None when the request is structurally sound."""
    if end <= start:
        return "effective_end must be after effective_start"
    if len(set(body.hubs)) != len(body.hubs):
        return "duplicate hub in hubs"
    if len(set(body.stations)) != len(body.stations):
        return "duplicate station in stations"
    unknown_hubs = sorted(set(body.hubs) - set(body.stations))
    if unknown_hubs:
        return f"hubs must themselves be served stations; unknown hubs: {unknown_hubs}"
    known = set(body.stations)
    for s in body.segments:
        s_start, s_end = normalize_ts(s.start_at), normalize_ts(s.end_at)
        if s_end <= s_start:
            return f"segment '{s.segment_id}' end_at must be after start_at"
        if s.from_station not in known:
            return f"segment '{s.segment_id}' references unknown from_station '{s.from_station}'"
        if s.to_station not in known:
            return f"segment '{s.segment_id}' references unknown to_station '{s.to_station}'"
    for t in body.transfers:
        if normalize_ts(t.end_at) <= normalize_ts(t.start_at):
            return "a walking transfer's end_at must be after start_at"
        if t.from_station not in known:
            return f"transfer references unknown from_station '{t.from_station}'"
        if t.to_station not in known:
            return f"transfer references unknown to_station '{t.to_station}'"
    return None


def _network_edges(body: NetworkValidateIn) -> list[NetEdge]:
    edges = [
        NetEdge(
            kind="segment",
            from_station=s.from_station,
            to_station=s.to_station,
            start=normalize_ts(s.start_at),
            end=normalize_ts(s.end_at),
            line_id=s.line_id,
            segment_id=s.segment_id,
        )
        for s in body.segments
    ]
    edges += [
        NetEdge(
            kind="transfer",
            from_station=t.from_station,
            to_station=t.to_station,
            start=normalize_ts(t.start_at),
            end=normalize_ts(t.end_at),
        )
        for t in body.transfers
    ]
    return edges


def _stored_validation_response(row, status: str) -> NetworkValidateOut:
    stored = json.loads(row["result"])
    stored["status"] = status  # the stored body always carries "created"
    return NetworkValidateOut(**stored)


@app.post("/network/validate", response_model=NetworkValidateOut, status_code=200)
def validate_transfer_network(
    body: NetworkValidateIn,
    db: Database = Depends(get_db),
) -> NetworkValidateOut:
    start = normalize_ts(body.effective_start)
    end = normalize_ts(body.effective_end)
    problem = _validate_network_input(body, start, end)
    if problem is not None:
        raise HTTPException(status_code=422, detail=problem)

    canonical_payload = _canonical_network_payload(body, start, end)
    payload_hash = hashlib.sha256(
        canonical_json(canonical_payload).encode("utf-8")
    ).hexdigest()

    with db.lock(), db.conn:
        existing = db.conn.execute(
            "SELECT * FROM network_validations WHERE validation_id = ?",
            (body.validation_id,),
        ).fetchone()
        if existing is not None:
            if existing["payload_hash"] != payload_hash:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"validation_id '{body.validation_id}' already exists"
                        " with a different payload"
                    ),
                )
            # Idempotent replay: the stored result is never recomputed; only
            # the replay marker is overwritten for the response.
            return _stored_validation_response(existing, "duplicate")

        report = validate_network(
            start, end, body.stations, body.hubs, _network_edges(body)
        )
        response = NetworkValidateOut(
            validation_id=body.validation_id,
            status="created",
            passed=report["conflict"] is None,
            effective_start=iso_str(start),
            effective_end=iso_str(end),
            slice_count=report["slice_count"],
            slices=report["slices"],
            conflict=report["conflict"],
        )

        db.conn.execute(
            "INSERT INTO network_validations (validation_id, payload_hash, payload,"
            " effective_start, effective_end, passed, result, created_at)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (
                body.validation_id,
                payload_hash,
                canonical_json(canonical_payload),
                iso_str(start),
                iso_str(end),
                1 if response.passed else 0,
                canonical_json(response.model_dump()),
                iso_str(datetime.now(timezone.utc)),
            ),
        )
        append_audit(
            db.conn,
            "network_validated",
            canonical_payload,
            response.model_dump(),
        )

    return response


@app.post("/network/snapshots", response_model=NetworkSnapshotOut, status_code=200)
def freeze_network_snapshot(
    body: NetworkSnapshotIn,
    db: Database = Depends(get_db),
) -> NetworkSnapshotOut:
    with db.lock(), db.conn:
        validation = db.conn.execute(
            "SELECT * FROM network_validations WHERE validation_id = ?",
            (body.validation_id,),
        ).fetchone()
        if validation is None:
            raise HTTPException(
                status_code=404,
                detail=f"validation_id '{body.validation_id}' not found; run POST /network/validate first",
            )

        existing = db.conn.execute(
            "SELECT * FROM network_snapshots WHERE snapshot_id = ?",
            (body.validation_id,),
        ).fetchone()
        if existing is not None:
            return _snapshot_response(existing, "duplicate")

        if not validation["passed"]:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"validation_id '{body.validation_id}' did not pass; only"
                    " validations without conflicts can be frozen"
                ),
            )

        frozen_at = iso_str(datetime.now(timezone.utc))
        # Copy the payload and result at freeze time: the snapshot is an
        # immutable artefact and no later validation can rewrite it.
        db.conn.execute(
            "INSERT INTO network_snapshots (snapshot_id, payload, result, created_at)"
            " VALUES (?,?,?,?)",
            (body.validation_id, validation["payload"], validation["result"], frozen_at),
        )
        row = db.conn.execute(
            "SELECT * FROM network_snapshots WHERE snapshot_id = ?",
            (body.validation_id,),
        ).fetchone()
        response = _snapshot_response(row, "created")
        append_audit(
            db.conn,
            "network_snapshot_created",
            {"validation_id": body.validation_id},
            response.model_dump(),
        )

    return response


def _snapshot_response(row, status: str) -> NetworkSnapshotOut:
    return NetworkSnapshotOut(
        snapshot_id=row["snapshot_id"],
        status=status,
        effective_start=json.loads(row["result"])["effective_start"],
        effective_end=json.loads(row["result"])["effective_end"],
        frozen_at=row["created_at"],
        validation=NetworkValidateOut(**json.loads(row["result"])),
    )


@app.get("/network/snapshots/{snapshot_id}", response_model=NetworkSnapshotOut)
def get_network_snapshot(
    snapshot_id: str,
    db: Database = Depends(get_db),
) -> NetworkSnapshotOut:
    with db.lock():
        row = db.conn.execute(
            "SELECT * FROM network_snapshots WHERE snapshot_id = ?",
            (snapshot_id,),
        ).fetchone()
    if row is None:
        raise HTTPException(
            status_code=404, detail=f"snapshot '{snapshot_id}' not found"
        )
    return _snapshot_response(row, "created")


# ---------------------------------------------------------------------------
# Fare disputes and refund settlement
# ---------------------------------------------------------------------------


@app.post("/disputes", response_model=DisputeOut, status_code=200)
def create_dispute(
    body: DisputeIn,
    db: Database = Depends(get_db),
) -> DisputeOut:
    canonical_payload = canonical_dispute_payload(body)
    p_hash = payload_hash(canonical_payload)

    with db.lock(), db.conn:
        existing = db.conn.execute(
            "SELECT * FROM disputes WHERE dispute_id = ?", (body.dispute_id,)
        ).fetchone()
        if existing is not None:
            if existing["payload_hash"] != p_hash:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"dispute_id '{body.dispute_id}' already exists"
                        " with a different payload"
                    ),
                )
            # Idempotent replay: the frozen snapshot and any settled refund
            # are never recomputed or rewritten.
            return dispute_response(db.conn, existing, "duplicate")

        charge = db.conn.execute(
            "SELECT t.passenger_id, c.amount_cents, c.base_fare_cents,"
            " c.transfer_discount_cents, c.basis"
            " FROM trip_charges c JOIN trips t ON t.trip_id = c.trip_id"
            " WHERE c.trip_id = ?",
            (body.trip_id,),
        ).fetchone()
        if charge is None:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"trip_id '{body.trip_id}' not found; only paired trips with"
                    " a settled charge can be disputed"
                ),
            )
        if charge["passenger_id"] != body.passenger_id:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"trip_id '{body.trip_id}' belongs to passenger"
                    f" '{charge['passenger_id']}', not '{body.passenger_id}'"
                ),
            )
        if body.claimed_amount_cents > charge["amount_cents"]:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"claimed_amount_cents {body.claimed_amount_cents} exceeds the"
                    f" frozen snapshot receivable {charge['amount_cents']}"
                ),
            )

        frozen_at = iso_str(datetime.now(timezone.utc))
        db.conn.execute(
            "INSERT INTO disputes (dispute_id, payload_hash, passenger_id, trip_id,"
            " claimed_amount_cents, reason, created_at) VALUES (?,?,?,?,?,?,?)",
            (
                body.dispute_id,
                p_hash,
                body.passenger_id,
                body.trip_id,
                body.claimed_amount_cents,
                body.reason,
                frozen_at,
            ),
        )
        # Freeze the charge snapshot at creation time: later event corrections
        # recompute trip_charges but never mutate this copy.
        db.conn.execute(
            "INSERT INTO dispute_snapshots (dispute_id, trip_id, passenger_id,"
            " amount_cents, base_fare_cents, transfer_discount_cents, basis, frozen_at)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (
                body.dispute_id,
                body.trip_id,
                charge["passenger_id"],
                charge["amount_cents"],
                charge["base_fare_cents"],
                charge["transfer_discount_cents"],
                charge["basis"],
                frozen_at,
            ),
        )
        row = db.conn.execute(
            "SELECT * FROM disputes WHERE dispute_id = ?", (body.dispute_id,)
        ).fetchone()
        response = dispute_response(db.conn, row, "created")
        append_audit(
            db.conn,
            "dispute_opened",
            canonical_payload,
            response.model_dump(),
        )

    return response


@app.post("/disputes/{dispute_id}/resolve", response_model=DisputeOut, status_code=200)
def resolve_dispute(
    dispute_id: str,
    body: DisputeResolveIn,
    db: Database = Depends(get_db),
) -> DisputeOut:
    canonical_payload = canonical_resolve_payload(body, dispute_id)
    p_hash = payload_hash(canonical_payload)

    with db.lock(), db.conn:
        dispute = db.conn.execute(
            "SELECT * FROM disputes WHERE dispute_id = ?", (dispute_id,)
        ).fetchone()
        if dispute is None:
            raise HTTPException(
                status_code=404, detail=f"dispute_id '{dispute_id}' not found"
            )

        existing_refund = db.conn.execute(
            "SELECT * FROM refunds WHERE dispute_id = ?", (dispute_id,)
        ).fetchone()
        if existing_refund is not None:
            if existing_refund["payload_hash"] != p_hash:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"dispute '{dispute_id}' is already resolved (version 2);"
                        " the settled refund is immutable and cannot be re-resolved"
                    ),
                )
            # Idempotent replay of the identical resolution.
            return dispute_response(db.conn, dispute, "duplicate")

        if body.expected_version != 1:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"version conflict: dispute '{dispute_id}' is at version 1"
                    f" (open); expected_version {body.expected_version} is stale"
                ),
            )
        if body.approved_amount_cents > dispute["claimed_amount_cents"]:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"approved_amount_cents {body.approved_amount_cents} exceeds the"
                    f" claimed amount {dispute['claimed_amount_cents']}"
                ),
            )

        resolved_at = iso_str(datetime.now(timezone.utc))
        rid = refund_id_for(dispute_id)
        # One immutable ledger row, written in the same transaction as the
        # audit record; the no-update/no-delete triggers guard it afterwards.
        db.conn.execute(
            "INSERT INTO refunds (refund_id, dispute_id, passenger_id, trip_id,"
            " amount_cents, payload_hash, review_note, created_at)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (
                rid,
                dispute_id,
                dispute["passenger_id"],
                dispute["trip_id"],
                body.approved_amount_cents,
                p_hash,
                body.review_note,
                resolved_at,
            ),
        )
        response = dispute_response(db.conn, dispute, "created")
        append_audit(
            db.conn,
            "dispute_resolved",
            canonical_payload,
            response.model_dump(),
        )

    return response


@app.get("/disputes/{dispute_id}", response_model=DisputeOut)
def get_dispute(
    dispute_id: str,
    db: Database = Depends(get_db),
) -> DisputeOut:
    with db.lock():
        dispute = db.conn.execute(
            "SELECT * FROM disputes WHERE dispute_id = ?", (dispute_id,)
        ).fetchone()
        if dispute is None:
            raise HTTPException(
                status_code=404, detail=f"dispute_id '{dispute_id}' not found"
            )
        # The live trip charge is read here, so corrections that happened
        # after resolution surface as top-up/clawback differences without ever
        # touching the frozen snapshot or the settled refund.
        return dispute_response(db.conn, dispute, "created")


# ---------------------------------------------------------------------------
# Operator fare clearing (运营方票款清分)
# ---------------------------------------------------------------------------


def _clearing_operator_totals(db: Database, root_clearing_id: str) -> list[ClearingOperatorTotal]:
    """Cumulative per-operator position booked across the whole chain (through
    every batch, including the just-written one), sorted by operator id."""
    rows = db.conn.execute(
        "SELECT operator_id, SUM(amount_cents) AS amount"
        " FROM clearing_entries e"
        " JOIN clearing_batches b ON b.clearing_id = e.clearing_id"
        " WHERE b.root_clearing_id = ?"
        " GROUP BY operator_id ORDER BY operator_id",
        (root_clearing_id,),
    ).fetchall()
    return [
        ClearingOperatorTotal(operator_id=r["operator_id"], amount_cents=r["amount"])
        for r in rows
    ]


def _load_clearing(db: Database, clearing_id: str, status: str) -> ClearingBatchOut:
    batch = db.conn.execute(
        "SELECT * FROM clearing_batches WHERE clearing_id = ?", (clearing_id,)
    ).fetchone()
    if batch is None:
        raise HTTPException(
            status_code=404, detail=f"clearing '{clearing_id}' not found"
        )
    rows = db.conn.execute(
        "SELECT seq, trip_id, passenger_id, from_station, to_station, ended_at,"
        " operator_id, basis_points, gross_amount_cents, amount_cents,"
        " prior_amount_cents, cumulative_amount_cents"
        " FROM clearing_entries WHERE clearing_id = ? ORDER BY seq",
        (clearing_id,),
    ).fetchall()
    rules = json.loads(batch["rules"]) if batch["rules"] else []
    return ClearingBatchOut(
        clearing_id=batch["clearing_id"],
        root_clearing_id=batch["root_clearing_id"],
        status=status,
        kind=batch["kind"],
        seq=batch["seq"],
        window_start=batch["window_start"],
        window_end=batch["window_end"],
        rules=rules,
        base_clearing_id=batch["base_clearing_id"],
        trip_count=batch["trip_count"],
        reversed_trip_count=batch["reversed_trip_count"],
        ledger_row_count=batch["ledger_row_count"],
        gross_amount_cents=batch["gross_amount_cents"],
        net_amount_cents=batch["net_amount_cents"],
        entries=[
            ClearingEntryOut(
                seq=r["seq"],
                trip_id=r["trip_id"],
                passenger_id=r["passenger_id"],
                from_station=r["from_station"],
                to_station=r["to_station"],
                ended_at=r["ended_at"],
                operator_id=r["operator_id"],
                basis_points=r["basis_points"],
                gross_amount_cents=r["gross_amount_cents"],
                amount_cents=r["amount_cents"],
                prior_amount_cents=r["prior_amount_cents"],
                cumulative_amount_cents=r["cumulative_amount_cents"],
                kind=batch["kind"],
            )
            for r in rows
        ],
        operator_totals=_clearing_operator_totals(db, batch["root_clearing_id"]),
        created_at=batch["created_at"],
    )


@app.post("/clearings", response_model=ClearingBatchOut, status_code=200)
def create_clearing(
    body: ClearingIn,
    db: Database = Depends(get_db),
) -> ClearingBatchOut:
    """First closing: freeze the window and operator rules, read the current
    in-window trip receivables, split every amount by basis points and write
    the immutable ledger."""
    window_start = normalize_ts(body.window_start)
    window_end = normalize_ts(body.window_end)
    if window_end <= window_start:
        raise HTTPException(status_code=422, detail="window_end must be after window_start")
    problem = validate_clearing_rules(body.rules)
    if problem is not None:
        raise HTTPException(status_code=422, detail=problem)

    rules = canonical_rules(body.rules)
    canonical_payload = canonical_initial_payload(
        body.clearing_id, window_start, window_end, rules
    )
    p_hash = clearing_payload_hash(canonical_payload)

    with db.lock(), db.conn:
        existing = db.conn.execute(
            "SELECT payload_hash FROM clearing_batches WHERE clearing_id = ?",
            (body.clearing_id,),
        ).fetchone()
        if existing is not None:
            if existing["payload_hash"] != p_hash:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"clearing_id '{body.clearing_id}' already exists with a"
                        " different payload"
                    ),
                )
            return _load_clearing(db, body.clearing_id, "duplicate")

        rules_by_pair = rule_map_from_canonical(rules)
        trips = load_clearing_trips(db.conn, window_start, window_end)
        missing = sorted(
            {f"{t['from_station']}->{t['to_station']}"
             for t in trips if (t["from_station"], t["to_station"]) not in rules_by_pair}
        )
        if missing:
            # Whole-batch rejection: no row is written when the frozen rules
            # do not cover every in-window trip.
            raise HTTPException(
                status_code=422,
                detail=(
                    "no matching operator rule for in-window trip station"
                    f" pair(s): {missing}"
                ),
            )

        created_at = iso_str(datetime.now(timezone.utc))
        entries: list[dict] = []
        gross_total = 0
        for trip in trips:
            shares = rules_by_pair[(trip["from_station"], trip["to_station"])]
            allocations = split_amount(trip["amount_cents"], shares)
            bp_by_operator = dict(shares)
            gross_total += trip["amount_cents"]
            for operator_id, basis_points in sorted(shares, key=lambda s: s[0]):
                entries.append({
                    "trip_id": trip["trip_id"],
                    "passenger_id": trip["passenger_id"],
                    "from_station": trip["from_station"],
                    "to_station": trip["to_station"],
                    "ended_at": trip["ended_at"],
                    "operator_id": operator_id,
                    "basis_points": basis_points,
                    "gross_amount_cents": trip["amount_cents"],
                    "amount_cents": allocations[operator_id],
                    "prior_amount_cents": 0,
                    "cumulative_amount_cents": allocations[operator_id],
                    "bp": bp_by_operator[operator_id],
                })
        entries.sort(key=lambda e: (e["ended_at"], e["trip_id"], e["operator_id"]))

        net_total = sum(e["amount_cents"] for e in entries)
        db.conn.execute(
            "INSERT INTO clearing_batches (clearing_id, root_clearing_id, seq, kind,"
            " payload_hash, window_start, window_end, rules, base_clearing_id,"
            " trip_count, reversed_trip_count, ledger_row_count, gross_amount_cents,"
            " net_amount_cents, created_at)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                body.clearing_id, body.clearing_id, 0, "initial",
                p_hash, iso_str(window_start), iso_str(window_end),
                canonical_json(rules), None,
                len(trips), 0, len(entries), gross_total, net_total, created_at,
            ),
        )
        db.conn.executemany(
            "INSERT INTO clearing_entries (clearing_id, seq, trip_id, passenger_id,"
            " from_station, to_station, ended_at, operator_id, basis_points,"
            " gross_amount_cents, amount_cents, prior_amount_cents,"
            " cumulative_amount_cents) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                (
                    body.clearing_id, seq, e["trip_id"], e["passenger_id"],
                    e["from_station"], e["to_station"], e["ended_at"],
                    e["operator_id"], e["bp"], e["gross_amount_cents"],
                    e["amount_cents"], e["prior_amount_cents"],
                    e["cumulative_amount_cents"],
                )
                for seq, e in enumerate(entries)
            ],
        )
        response = _load_clearing(db, body.clearing_id, "created")
        append_audit(db.conn, "clearing_closed", canonical_payload, response.model_dump())

    return response


@app.post("/clearings/adjustments", response_model=ClearingBatchOut, status_code=200)
def create_clearing_adjustment(
    body: ClearingAdjustmentIn,
    db: Database = Depends(get_db),
) -> ClearingBatchOut:
    """Follow-up batch after tap corrections. The first closing's frozen
    window and rules still apply: target per-operator amounts are split from
    the current charges and reduced by everything already booked, so only
    signed deltas (including full reversals of vanished trips) are appended."""
    canonical_payload = canonical_adjustment_payload(
        body.clearing_id, body.base_clearing_id
    )
    p_hash = clearing_payload_hash(canonical_payload)

    with db.lock(), db.conn:
        existing = db.conn.execute(
            "SELECT payload_hash FROM clearing_batches WHERE clearing_id = ?",
            (body.clearing_id,),
        ).fetchone()
        if existing is not None:
            if existing["payload_hash"] != p_hash:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"clearing_id '{body.clearing_id}' already exists with a"
                        " different payload"
                    ),
                )
            return _load_clearing(db, body.clearing_id, "duplicate")

        root = db.conn.execute(
            "SELECT * FROM clearing_batches WHERE clearing_id = ?",
            (body.base_clearing_id,),
        ).fetchone()
        if root is None:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"base_clearing_id '{body.base_clearing_id}' not found; run"
                    " POST /clearings for the first closing first"
                ),
            )
        if root["kind"] != "initial" or root["seq"] != 0:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"base_clearing_id '{body.base_clearing_id}' is an adjustment"
                    " batch; adjustments must reference the first closing"
                ),
            )

        window_start = normalize_ts(
            datetime.fromisoformat(root["window_start"].replace("Z", "+00:00"))
        )
        window_end = normalize_ts(
            datetime.fromisoformat(root["window_end"].replace("Z", "+00:00"))
        )
        rules = json.loads(root["rules"])
        rules_by_pair = rule_map_from_canonical(rules)

        next_seq = db.conn.execute(
            "SELECT COALESCE(MAX(seq), 0) + 1 FROM clearing_batches"
            " WHERE root_clearing_id = ?",
            (body.base_clearing_id,),
        ).fetchone()[0]
        prior = prior_operator_totals(db.conn, body.base_clearing_id, next_seq)
        seen_before = last_seen_trips(db.conn, body.base_clearing_id, next_seq)

        current_trips = load_clearing_trips(db.conn, window_start, window_end)
        missing = sorted(
            {f"{t['from_station']}->{t['to_station']}"
             for t in current_trips
             if (t["from_station"], t["to_station"]) not in rules_by_pair}
        )
        if missing:
            # The first closing's rules stay frozen; a correction must never
            # leave a current trip outside their coverage. Open a new first
            # closing (with new rules) instead.
            raise HTTPException(
                status_code=422,
                detail=(
                    "no matching frozen operator rule for current in-window trip"
                    f" station pair(s): {missing}; the first closing's rules are"
                    " immutable -- open a new first closing with a new clearing_id"
                    " to settle under different rules"
                ),
            )

        current_ids = {t["trip_id"] for t in current_trips}
        vanished_ids = sorted(set(seen_before) - current_ids)

        entries: list[dict] = []
        gross_total = 0
        reversed_count = 0
        prior_ops_by_trip: dict[str, set[str]] = {}
        for (tid, op) in prior:
            prior_ops_by_trip.setdefault(tid, set()).add(op)
        for trip in current_trips:
            shares = rules_by_pair[(trip["from_station"], trip["to_station"])]
            targets = split_amount(trip["amount_cents"], shares)
            gross_total += trip["amount_cents"]
            # Include operators booked under the trip's previous station pair:
            # a correction can move the same trip_id onto a different pair with
            # a different operator set, and the operators dropped by the new
            # rule must be reversed to zero (target 0) rather than left booked.
            all_operators = {op for op, _bp in shares} | prior_ops_by_trip.get(
                trip["trip_id"], set()
            )
            for operator_id in sorted(all_operators):
                prev_amount = prior.get((trip["trip_id"], operator_id), 0)
                target = targets.get(operator_id, 0)
                delta = target - prev_amount
                if delta == 0:
                    continue
                entries.append({
                    "trip_id": trip["trip_id"],
                    "passenger_id": trip["passenger_id"],
                    "from_station": trip["from_station"],
                    "to_station": trip["to_station"],
                    "ended_at": trip["ended_at"],
                    "operator_id": operator_id,
                    "basis_points": None,
                    "gross_amount_cents": trip["amount_cents"],
                    "amount_cents": delta,
                    "prior_amount_cents": prev_amount,
                    "cumulative_amount_cents": prev_amount + delta,
                })
        for trip_id in vanished_ids:
            last = seen_before[trip_id]
            # Reverse whatever earlier batches booked, per operator, so the
            # disappeared trip nets to zero.
            operators = sorted(
                op for (tid, op) in prior if tid == trip_id and prior[(tid, op)] != 0
            )
            if operators:
                reversed_count += 1
            for operator_id in operators:
                prev_amount = prior[(trip_id, operator_id)]
                entries.append({
                    "trip_id": trip_id,
                    "passenger_id": last["passenger_id"],
                    "from_station": last["from_station"],
                    "to_station": last["to_station"],
                    "ended_at": last["ended_at"],
                    "operator_id": operator_id,
                    "basis_points": None,
                    "gross_amount_cents": 0,
                    "amount_cents": -prev_amount,
                    "prior_amount_cents": prev_amount,
                    "cumulative_amount_cents": 0,
                })
        entries.sort(key=lambda e: (e["ended_at"], e["trip_id"], e["operator_id"]))

        net_total = sum(e["amount_cents"] for e in entries)
        created_at = iso_str(datetime.now(timezone.utc))
        # Batch row and its immutable delta rows commit together, in the same
        # transaction as the audit record.
        db.conn.execute(
            "INSERT INTO clearing_batches (clearing_id, root_clearing_id, seq, kind,"
            " payload_hash, window_start, window_end, rules, base_clearing_id,"
            " trip_count, reversed_trip_count, ledger_row_count, gross_amount_cents,"
            " net_amount_cents, created_at)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                body.clearing_id, body.base_clearing_id, next_seq, "adjustment",
                p_hash, iso_str(window_start), iso_str(window_end),
                root["rules"], body.base_clearing_id,
                len(current_trips), reversed_count, len(entries),
                gross_total, net_total, created_at,
            ),
        )
        db.conn.executemany(
            "INSERT INTO clearing_entries (clearing_id, seq, trip_id, passenger_id,"
            " from_station, to_station, ended_at, operator_id, basis_points,"
            " gross_amount_cents, amount_cents, prior_amount_cents,"
            " cumulative_amount_cents) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                (
                    body.clearing_id, seq, e["trip_id"], e["passenger_id"],
                    e["from_station"], e["to_station"], e["ended_at"],
                    e["operator_id"], e["basis_points"], e["gross_amount_cents"],
                    e["amount_cents"], e["prior_amount_cents"],
                    e["cumulative_amount_cents"],
                )
                for seq, e in enumerate(entries)
            ],
        )
        response = _load_clearing(db, body.clearing_id, "created")
        append_audit(db.conn, "clearing_closed", canonical_payload, response.model_dump())

    return response


@app.get("/clearings/{clearing_id}", response_model=ClearingBatchOut)
def get_clearing(
    clearing_id: str,
    db: Database = Depends(get_db),
) -> ClearingBatchOut:
    with db.lock():
        # The replay marker only appears on POST responses; a stored batch is
        # always reported as "created", while the frozen window/rules and the
        # chain-wide operator positions are returned as they stand now.
        return _load_clearing(db, clearing_id, "created")


# ---------------------------------------------------------------------------
# Tap-event quality scans (read-only analysis, isolated append-only tables)
# ---------------------------------------------------------------------------


def _scan_response(row, alerts: list[dict], status: str) -> EventQualityScanOut:
    return EventQualityScanOut(
        scan_id=row["scan_id"],
        status=status,
        window_start=row["window_start"],
        window_end=row["window_end"],
        passengers=json.loads(row["passengers"]) if row["passengers"] is not None else None,
        duplicate_window_seconds=row["duplicate_window_seconds"],
        min_travel_seconds=json.loads(row["min_travel_seconds"]),
        event_count=row["event_count"],
        alert_count=row["alert_count"],
        alerts=[QualityAlert(**a) for a in alerts],
    )


def _load_scan(db: Database, scan_id: str) -> tuple:
    scan = db.conn.execute(
        "SELECT * FROM event_quality_scans WHERE scan_id = ?", (scan_id,)
    ).fetchone()
    if scan is None:
        raise HTTPException(
            status_code=404, detail=f"event quality scan '{scan_id}' not found"
        )
    rows = db.conn.execute(
        "SELECT event_id, passenger_id, rule, actual_interval_seconds,"
        " threshold_seconds, evidence FROM event_quality_alerts"
        " WHERE scan_id = ? ORDER BY seq",
        (scan_id,),
    ).fetchall()
    alerts = []
    for r in rows:
        a = {
            "event_id": r["event_id"],
            "passenger_id": r["passenger_id"],
            "rule": r["rule"],
            "actual_interval_seconds": r["actual_interval_seconds"],
            "threshold_seconds": r["threshold_seconds"],
            "evidence": [
                QualityEvidenceEvent(**e) for e in json.loads(r["evidence"])
            ],
        }
        alerts.append(a)
    return scan, alerts


@app.post("/event-quality/scans", response_model=EventQualityScanOut, status_code=200)
def create_event_quality_scan(
    body: EventQualityScanIn,
    db: Database = Depends(get_db),
) -> EventQualityScanOut:
    window_start = normalize_ts(body.window_start)
    window_end = normalize_ts(body.window_end)
    if window_end <= window_start:
        raise HTTPException(status_code=422, detail="window_end must be after window_start")
    passengers = sorted(set(body.passengers)) if body.passengers is not None else None
    for out_station, pairs in body.min_travel_seconds.items():
        if not pairs:
            raise HTTPException(
                status_code=422,
                detail=f"min_travel_seconds['{out_station}'] must not be empty",
            )
        for in_station, seconds in pairs.items():
            if seconds < 0:
                raise HTTPException(
                    status_code=422,
                    detail="min_travel_seconds values must be >= 0",
                )

    canonical_payload = canonical_scan_payload(
        body.scan_id,
        window_start,
        window_end,
        passengers,
        body.duplicate_window_seconds,
        body.min_travel_seconds,
    )
    p_hash = scan_payload_hash(canonical_payload)

    with db.lock(), db.conn:
        existing = db.conn.execute(
            "SELECT * FROM event_quality_scans WHERE scan_id = ?", (body.scan_id,)
        ).fetchone()
        if existing is not None:
            if existing["payload_hash"] != p_hash:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"scan_id '{body.scan_id}' already exists with a different"
                        " payload"
                    ),
                )
            # Idempotent replay: the stored scan result is never recomputed;
            # scans see the same frozen evidence they produced.
            row, alerts = _load_scan(db, body.scan_id)
            return _scan_response(row, alerts, "duplicate")

        grouped = load_window_events(
            db.conn, window_start, window_end, passengers
        )
        alerts: list = []
        event_count = 0
        for pid in sorted(grouped):
            per_passenger = scan_events(
                grouped[pid],
                duplicate_window_seconds=body.duplicate_window_seconds,
                min_travel_seconds=body.min_travel_seconds,
            )
            event_count += len(grouped[pid])
            alerts.extend(per_passenger)

        # Merge the per-passenger sequences into one global scan ordering.
        alerts.sort(key=lambda a: (
            a.evidence[-1]["occurred_at"],
            a.evidence[-1]["seq"],
            a.passenger_id,
            a.rule,
        ))
        for i, a in enumerate(alerts):
            a.seq = i

        created_at = iso_str(datetime.now(timezone.utc))
        db.conn.execute(
            "INSERT INTO event_quality_scans (scan_id, payload_hash, window_start,"
            " window_end, passengers, duplicate_window_seconds, min_travel_seconds,"
            " event_count, alert_count, created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                body.scan_id,
                p_hash,
                iso_str(window_start),
                iso_str(window_end),
                json.dumps(passengers) if passengers is not None else None,
                body.duplicate_window_seconds,
                canonical_json(canonical_payload["min_travel_seconds"]),
                event_count,
                len(alerts),
                created_at,
            ),
        )
        db.conn.executemany(
            "INSERT INTO event_quality_alerts (scan_id, seq, event_id, passenger_id,"
            " rule, actual_interval_seconds, threshold_seconds, evidence)"
            " VALUES (?,?,?,?,?,?,?,?)",
            [
                (
                    body.scan_id,
                    a.seq,
                    a.event_id,
                    a.passenger_id,
                    a.rule,
                    a.actual_interval_seconds,
                    a.threshold_seconds,
                    canonical_json(a.evidence),
                )
                for a in alerts
            ],
        )
        # Scans are read-only analysis: no audit record is appended, and
        # events/trips/charges/disputes are never written.
        row, stored_alerts = _load_scan(db, body.scan_id)

    return _scan_response(row, stored_alerts, "created")


@app.get("/event-quality/scans/{scan_id}", response_model=EventQualityScanOut)
def get_event_quality_scan(
    scan_id: str,
    db: Database = Depends(get_db),
) -> EventQualityScanOut:
    with db.lock():
        row, alerts = _load_scan(db, scan_id)
    return _scan_response(row, alerts, "created")


# ---------------------------------------------------------------------------
# Anonymous k-anonymous trajectory exports (read-only analysis, isolated
# append-only tables; no audit records)
# ---------------------------------------------------------------------------


def _export_response(row, trip_rows, status: str) -> PrivacyExportOut:
    classes = classes_from_trip_rows(trip_rows)
    return PrivacyExportOut(
        export_id=row["export_id"],
        status=status,
        window_start=row["window_start"],
        window_end=row["window_end"],
        bucket_minutes=row["bucket_minutes"],
        k=row["k"],
        station_groups=load_station_groups(row["station_groups"]),
        input_trip_count=row["input_trip_count"],
        equivalence_class_count=row["equivalence_class_count"],
        retained_class_count=row["retained_class_count"],
        suppressed_class_count=row["suppressed_class_count"],
        retained_trip_count=row["retained_trip_count"],
        suppressed_trip_count=row["suppressed_trip_count"],
        classes=[PrivacyEquivalenceClass(**c) for c in classes],
        trips=[
            PrivacyExportTrip(
                pseudonym=r["pseudonym"],
                bucket_start=r["bucket_start"],
                origin_group=r["origin_group"],
                destination_group=r["destination_group"],
            )
            for r in trip_rows
        ],
    )


def _load_export(db: Database, export_id: str):
    row = db.conn.execute(
        "SELECT * FROM privacy_exports WHERE export_id = ?", (export_id,)
    ).fetchone()
    if row is None:
        raise HTTPException(
            status_code=404, detail=f"privacy export '{export_id}' not found"
        )
    trip_rows = db.conn.execute(
        "SELECT pseudonym, bucket_start, origin_group, destination_group"
        " FROM privacy_export_trips WHERE export_id = ? ORDER BY seq",
        (export_id,),
    ).fetchall()
    return row, trip_rows


@app.post("/privacy/exports", response_model=PrivacyExportOut, status_code=200)
def create_privacy_export(
    body: PrivacyExportIn,
    request: Request,
    db: Database = Depends(get_db),
) -> PrivacyExportOut:
    window_start = normalize_ts(body.window_start)
    window_end = normalize_ts(body.window_end)
    if window_end <= window_start:
        raise HTTPException(status_code=422, detail="window_end must be after window_start")
    station_groups = dict(body.station_groups)

    canonical_payload = canonical_export_payload(
        body.export_id,
        window_start,
        window_end,
        body.bucket_minutes,
        body.k,
        station_groups,
    )
    p_hash = export_payload_hash(canonical_payload)

    with db.lock(), db.conn:
        existing = db.conn.execute(
            "SELECT * FROM privacy_exports WHERE export_id = ?", (body.export_id,)
        ).fetchone()
        if existing is not None:
            if existing["payload_hash"] != p_hash:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"export_id '{body.export_id}' already exists with a"
                        " different payload"
                    ),
                )
            # Idempotent replay: the frozen pseudonyms and suppression
            # decisions are never recomputed.
            row, trip_rows = _load_export(db, body.export_id)
            return _export_response(row, trip_rows, "duplicate")

        trips = load_window_trips(db.conn, window_start, window_end)
        try:
            result = build_export(
                trips,
                station_groups,
                body.bucket_minutes,
                body.k,
                request.app.state.privacy_salt,
            )
        except UnmappedStationsError as exc:
            raise HTTPException(
                status_code=422,
                detail=(
                    "every in-window trip's origin and destination station must"
                    f" appear in station_groups; unmapped: {exc.stations}"
                ),
            )

        created_at = iso_str(datetime.now(timezone.utc))
        db.conn.execute(
            "INSERT INTO privacy_exports (export_id, payload_hash, window_start,"
            " window_end, bucket_minutes, k, station_groups, input_trip_count,"
            " equivalence_class_count, retained_class_count, suppressed_class_count,"
            " retained_trip_count, suppressed_trip_count, created_at)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                body.export_id,
                p_hash,
                iso_str(window_start),
                iso_str(window_end),
                body.bucket_minutes,
                body.k,
                stored_groups_json(station_groups),
                result["input_trip_count"],
                result["equivalence_class_count"],
                result["retained_class_count"],
                result["suppressed_class_count"],
                result["retained_trip_count"],
                result["suppressed_trip_count"],
                created_at,
            ),
        )
        db.conn.executemany(
            "INSERT INTO privacy_export_trips (export_id, seq, pseudonym,"
            " bucket_start, origin_group, destination_group)"
            " VALUES (?,?,?,?,?,?)",
            [
                (
                    body.export_id,
                    seq,
                    t["pseudonym"],
                    t["bucket_start"],
                    t["origin_group"],
                    t["destination_group"],
                )
                for seq, t in enumerate(result["trips"])
            ],
        )
        # Exports are read-only analysis: no audit record is appended, and
        # events/trips/charges/disputes are never written.
        row, trip_rows = _load_export(db, body.export_id)

    return _export_response(row, trip_rows, "created")


@app.get("/privacy/exports/{export_id}", response_model=PrivacyExportOut)
def get_privacy_export(
    export_id: str,
    db: Database = Depends(get_db),
) -> PrivacyExportOut:
    with db.lock():
        row, trip_rows = _load_export(db, export_id)
    return _export_response(row, trip_rows, "created")


# ---------------------------------------------------------------------------
# Sudden-disruption evacuation replanning (read-only analysis, isolated
# append-only tables; no audit records)
# ---------------------------------------------------------------------------


def _replan_response(row, status: str) -> DisruptionReplanOut:
    stored = json.loads(row["result"])
    stored["status"] = status  # the stored body always carries "created"
    return DisruptionReplanOut(**stored)


@app.post("/disruption/replans", response_model=DisruptionReplanOut, status_code=200)
def create_disruption_replan(
    body: DisruptionReplanIn,
    db: Database = Depends(get_db),
) -> DisruptionReplanOut:
    # Structural timetable checks are rejected before any persistence:
    # duplicate service/group ids, repeated stops inside one service, and
    # non-monotonic arrival/departure times never leave a row behind.
    try:
        min_transfer_seconds, services, groups = normalise_input(body)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    canonical_payload = canonical_replan_payload(
        body.replan_id, min_transfer_seconds, services, groups
    )
    p_hash = replan_payload_hash(canonical_payload)

    with db.lock(), db.conn:
        existing = db.conn.execute(
            "SELECT * FROM disruption_replans WHERE replan_id = ?",
            (body.replan_id,),
        ).fetchone()
        if existing is not None:
            if existing["payload_hash"] != p_hash:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"replan_id '{body.replan_id}' already exists with a"
                        " different payload"
                    ),
                )
            # Idempotent replay: the frozen allocation is never recomputed.
            return _replan_response(existing, "duplicate")

        result = build_replan(min_transfer_seconds, services, groups)
        created_at = iso_str(datetime.now(timezone.utc))
        db.conn.execute(
            "INSERT INTO disruption_replans (replan_id, payload_hash, payload,"
            " min_transfer_seconds, group_count, assigned_count,"
            " stranded_count, result, created_at)"
            " VALUES (?,?,?,?,?,?,?,?,?)",
            (
                body.replan_id,
                p_hash,
                canonical_json(canonical_payload),
                min_transfer_seconds,
                result["summary"]["group_count"],
                result["summary"]["assigned_count"],
                result["summary"]["stranded_count"],
                canonical_json(
                    {"replan_id": body.replan_id, "status": "created", **result}
                ),
                created_at,
            ),
        )
        # Replans are read-only analysis computed solely from the request:
        # no business table is touched and no audit record is appended.
        row = db.conn.execute(
            "SELECT * FROM disruption_replans WHERE replan_id = ?",
            (body.replan_id,),
        ).fetchone()

    return _replan_response(row, "created")


@app.get("/disruption/replans/{replan_id}", response_model=DisruptionReplanOut)
def get_disruption_replan(
    replan_id: str,
    db: Database = Depends(get_db),
) -> DisruptionReplanOut:
    with db.lock():
        row = db.conn.execute(
            "SELECT * FROM disruption_replans WHERE replan_id = ?",
            (replan_id,),
        ).fetchone()
    if row is None:
        raise HTTPException(
            status_code=404, detail=f"disruption replan '{replan_id}' not found"
        )
    return _replan_response(row, "created")


@app.get("/audit", response_model=AuditList)
def list_audit(
    operation: str | None = Query(
        default=None,
        pattern="^(event_processed|event_corrected|recon_batch_created"
                "|network_validated|network_snapshot_created"
                "|dispute_opened|dispute_resolved|clearing_closed"
                "|event_held|event_hold_released)$",
        description="Filter by operation type",
    ),
    after: int = Query(default=0, ge=0, description="Return records with seq > after"),
    limit: int = Query(default=100, ge=1, le=1000),
    db: Database = Depends(get_db),
) -> AuditList:
    sql = (
        "SELECT seq, operation, occurred_at, request_summary, result_summary,"
        " prev_hash, record_hash FROM audit_log WHERE seq > ?"
    )
    params: list = [after]
    if operation:
        sql += " AND operation = ?"
        params.append(operation)
    sql += " ORDER BY seq LIMIT ?"
    params.append(limit)
    with db.lock():
        rows = db.conn.execute(sql, params).fetchall()
    entries = [
        AuditEntry(
            seq=r["seq"],
            operation=r["operation"],
            occurred_at=r["occurred_at"],
            request_summary=json.loads(r["request_summary"]),
            result_summary=json.loads(r["result_summary"]),
            prev_hash=r["prev_hash"],
            record_hash=r["record_hash"],
        )
        for r in rows
    ]
    return AuditList(count=len(entries), entries=entries)


@app.get("/audit/verify", response_model=AuditVerification)
def audit_verify(db: Database = Depends(get_db)) -> AuditVerification:
    with db.lock():
        report = verify_chain(db.conn)
    return AuditVerification(**report)


# ---------------------------------------------------------------------------
# Online database backup (administration)
# ---------------------------------------------------------------------------


def _remove_backup_artifacts(path: str) -> None:
    """Best-effort cleanup of a temp backup file and any SQLite sidecars."""
    for suffix in ("", "-wal", "-shm", "-journal"):
        try:
            os.remove(path + suffix)
        except FileNotFoundError:
            pass
        except OSError:
            pass


def _verify_backup_file(path: str) -> None:
    """Open the freshly written copy with an independent connection and run
    PRAGMA integrity_check (and foreign_key_check). Raises on any defect."""
    check = sqlite3.connect(path, timeout=30)
    try:
        check.execute("PRAGMA query_only=ON")
        rows = check.execute("PRAGMA integrity_check").fetchall()
        if len(rows) != 1 or rows[0][0] != "ok":
            detail = "; ".join(r[0] for r in rows) or "integrity_check returned no rows"
            raise sqlite3.DatabaseError(f"backup integrity_check failed: {detail}")
        fk_violations = check.execute("PRAGMA foreign_key_check").fetchall()
        if fk_violations:
            raise sqlite3.DatabaseError(
                f"backup foreign_key_check found {len(fk_violations)} violation(s)"
            )
    finally:
        check.close()


def _sha256_file(path: str) -> tuple[str, int]:
    h = hashlib.sha256()
    size = 0
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
            size += len(chunk)
    return h.hexdigest(), size


@app.get("/admin/backup")
def admin_backup(db: Database = Depends(get_db)) -> FileResponse:
    """Online, consistent SQLite backup while the service keeps serving writes.

    The snapshot is produced with the SQLite backup API from an independent
    read-only connection (the live database file is never copied directly,
    and the application write lock is not held), then reopened with another
    independent connection for PRAGMA integrity_check. Only after the check
    passes is the file streamed back with its SHA-256 digest in a response
    header. The temporary file is removed after the response is sent, or as
    soon as any step fails.
    """
    if db.path == ":memory:":
        tmp_dir = None  # tempfile.mkdtemp-style default temp directory
    else:
        tmp_dir = os.path.dirname(os.path.abspath(db.path))
    fd, tmp_path = tempfile.mkstemp(prefix="transit-backup-", suffix=".db", dir=tmp_dir)
    os.close(fd)  # sqlite opens the path itself
    try:
        db.online_backup(
            tmp_path,
            pages=BACKUP_PAGES_PER_BATCH,
            progress=BACKUP_PROGRESS_CALLBACK,
        )
        _verify_backup_file(tmp_path)
        digest, size = _sha256_file(tmp_path)
    except Exception as exc:  # any failure: clean up and report explicitly
        _remove_backup_artifacts(tmp_path)
        raise HTTPException(
            status_code=500,
            detail=f"database backup failed: {exc}",
        )

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    headers = {
        "X-Backup-SHA256": digest,
        "X-Backup-Size-Bytes": str(size),
    }
    return FileResponse(
        tmp_path,
        media_type="application/vnd.sqlite3",
        filename=f"transit-backup-{stamp}.db",
        headers=headers,
        background=BackgroundTask(_remove_backup_artifacts, tmp_path),
    )
