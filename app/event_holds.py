"""Tap-event review holds (刷卡事件复核暂存与释放).

A reviewer stages one or more of a passenger's tap events under a ``hold_id``.
Inside a single transaction the service confirms every event belongs to that
passenger, freezes an immutable copy of each event's complete payload and marks
the events ``held``: they drop out of pairing, billing and the pending query
until the hold is released. A hold is refused as a whole (409, no state change)
when any of the events is already referenced by a reconciliation batch, a
clearing batch or a resolved dispute, or when any of the events is already held.

Holds use the same optimistic-concurrency shape as disputes: the immutable
``event_holds`` row plus its frozen ``event_hold_snapshots`` are version 1
(held); the append-only ``event_hold_releases`` row marks version 2 (released).
Releasing requires the expected version and re-checks every frozen event
payload hash, so a correction that raced the review (such corrections are
refused while held, but the hash check is the storage-level guarantee) makes
the release abort and leave the events held. On release the passenger is
re-settled in the original ``(occurred_at, seq)`` order and the signed per-trip
deltas are frozen onto the release row; holds and snapshots are never updated.
"""
from __future__ import annotations

import hashlib
import json

from .audit import canonical_json
from .models import EventHoldOut, HeldEventSnapshot, HoldReleaseResult

RELEASE_ID_PREFIX = "RL-"

HOLD_VERSION = 1
RELEASED_VERSION = 2


def release_id_for(hold_id: str) -> str:
    return f"{RELEASE_ID_PREFIX}{hold_id}"


def canonical_hold_payload(body) -> dict:
    """Canonical hold request: the event id list is sorted and de-duplicated,
    so semantically identical submissions hash identically regardless of array
    order (duplicate ids in one request are rejected as 422 upstream)."""
    return {
        "hold_id": body.hold_id,
        "passenger_id": body.passenger_id,
        "event_ids": sorted(set(body.event_ids)),
        "review_reason": body.review_reason,
    }


def canonical_release_payload(body, hold_id: str) -> dict:
    """Canonical release request. ``hold_id`` comes from the path, so it is
    included in the hash to make the release row self-describing."""
    return {
        "hold_id": hold_id,
        "expected_version": body.expected_version,
    }


def payload_hash(payload: dict) -> str:
    return hashlib.sha256(
        canonical_json(payload).encode("utf-8")
    ).hexdigest()


def canonical_event_payload(
    event_id: str,
    passenger_id: str,
    station_id: str,
    tap_type: str,
    occurred_at: str,
) -> dict:
    """Canonical single-event payload, used for the frozen per-event hash. The
    stored ``occurred_at`` is already normalised to UTC by POST /events."""
    return {
        "event_id": event_id,
        "passenger_id": passenger_id,
        "station_id": station_id,
        "tap_type": tap_type,
        "occurred_at": occurred_at,
    }


def event_payload_hash(row) -> str:
    return payload_hash(
        canonical_event_payload(
            row["event_id"],
            row["passenger_id"],
            row["station_id"],
            row["tap_type"],
            row["occurred_at"],
        )
    )


def referenced_trip_ids(conn) -> set[str]:
    """Trip ids that immutable downstream artefacts already reference: every
    reconciliation result row, every clearing ledger row and every settled
    dispute's refund. Open disputes are not booked artefacts and do not block
    a hold."""
    ids: set[str] = set()
    ids.update(
        r[0]
        for r in conn.execute(
            "SELECT DISTINCT trip_id FROM recon_results WHERE trip_id IS NOT NULL"
        )
    )
    ids.update(
        r[0] for r in conn.execute("SELECT DISTINCT trip_id FROM clearing_entries")
    )
    ids.update(
        r[0] for r in conn.execute("SELECT DISTINCT trip_id FROM refunds")
    )
    return ids


def event_references_trip(event_id: str, trip_id: str) -> bool:
    """Trip ids are ``"<in_event_id>:<out_event_id>"``; match on the colon
    boundary so an event is referenced when it is either leg of the trip."""
    return trip_id.startswith(f"{event_id}:") or trip_id.endswith(f":{event_id}")


def referenced_event_ids(conn, event_ids) -> list[str]:
    """Return the requested event ids that already appear in a booked trip, in
    canonical (sorted) order."""
    trip_ids = referenced_trip_ids(conn)
    return sorted(
        eid
        for eid in event_ids
        if any(event_references_trip(eid, tid) for tid in trip_ids)
    )


def _snapshot_model(row) -> HeldEventSnapshot:
    return HeldEventSnapshot(
        seq=row["seq"],
        event_id=row["event_id"],
        passenger_id=row["passenger_id"],
        station_id=row["station_id"],
        tap_type=row["tap_type"],
        occurred_at=row["occurred_at"],
        event_seq=row["event_seq"],
        payload_hash=row["payload_hash"],
        frozen_at=row["frozen_at"],
    )


def _release_model(release_row) -> HoldReleaseResult:
    rel = json.loads(release_row["result"])
    return HoldReleaseResult(**rel)


def build_hold_view(
    *,
    hold_id: str,
    passenger_id: str,
    review_reason: str,
    event_ids,
    created_at: str,
    snapshot_rows,
    hold_adjustments=None,
) -> EventHoldOut:
    """Assemble the held-state API view from its components. Used when a hold
    is first created (before the immutable row exists, so its result can be
    frozen onto that row in the same INSERT). Stored holds are read back with
    ``hold_response``, which overlays the release if one exists."""
    adjustments = list(hold_adjustments or [])
    return EventHoldOut(
        hold_id=hold_id,
        status="created",
        state="held",
        version=HOLD_VERSION,
        passenger_id=passenger_id,
        event_ids=list(event_ids),
        review_reason=review_reason,
        created_at=created_at,
        released_at=None,
        snapshots=[_snapshot_model(s) for s in snapshot_rows],
        release=None,
        adjustments=adjustments,
        trip_change_count=len(adjustments),
    )


def hold_response(conn, hold_row, status: str) -> EventHoldOut:
    """Build the API view for a stored hold. ``status`` is the request replay
    marker ("created" or "duplicate"); the persistent state is derived from
    whether the immutable release row exists, never from a mutable column."""
    stored = json.loads(hold_row["result"])  # frozen hold-time response body
    stored["status"] = status

    release_row = conn.execute(
        "SELECT * FROM event_hold_releases WHERE hold_id = ?",
        (hold_row["hold_id"],),
    ).fetchone()
    if release_row is not None:
        release = _release_model(release_row)
        stored["state"] = "released"
        stored["version"] = RELEASED_VERSION
        stored["released_at"] = release.released_at
        stored["release"] = release.model_dump()
    else:
        stored["state"] = "held"
        stored["version"] = HOLD_VERSION
        stored["released_at"] = None
        stored["release"] = None
    return EventHoldOut(**stored)
