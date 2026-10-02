"""Tap-event quality scanning.

A scan is a *read-only* analysis of stored tap events with results written to
isolated, append-only tables (``event_quality_scans`` /
``event_quality_alerts``). It never mutates events, trips, trip charges,
disputes or the audit log.

Events whose ``occurred_at`` falls in the half-open UTC window
``[window_start, window_end)`` are scanned in ``(occurred_at, seq)`` order,
per passenger (optionally restricted to an allow-list). Each event is checked
against the passenger's immediately preceding event:

* ``duplicate_tap``: adjacent same-type taps (in->in or out->out, i.e. an
  action repeated without the complementary action) occur within
  ``duplicate_window_seconds`` (inclusive); the alert is raised on the later
  tap, with an actual interval equal to the gap between the two taps.
* ``min_travel_violation``: after a tap-out at station A the passenger taps in
  at station B before the directed minimum travel time ``A -> B`` from
  ``min_travel_seconds`` has elapsed (strictly below the threshold).

Each alert carries the triggering event id, the rule, the actual interval in
seconds and an ordered evidence chain of the two supporting tap events.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime

from .audit import canonical_json
from .settlement import Event, iso_str, normalize_ts

DUPLICATE_RULE = "duplicate_tap"
MIN_TRAVEL_RULE = "min_travel_violation"


@dataclass
class Alert:
    seq: int
    event_id: str
    passenger_id: str
    rule: str
    actual_interval_seconds: float
    threshold_seconds: int
    evidence: list[dict]


def _evidence_event(ev: Event) -> dict:
    return {
        "event_id": ev.event_id,
        "passenger_id": ev.passenger_id,
        "station_id": ev.station_id,
        "tap_type": ev.tap_type,
        "occurred_at": iso_str(ev.occurred_at),
        "seq": ev.seq,
    }


def scan_events(
    events: list[Event],
    *,
    duplicate_window_seconds: int,
    min_travel_seconds: dict[str, dict[str, int]],
) -> list[Alert]:
    """Scan one passenger's events, already ordered by (occurred_at, seq).

    Each event is compared with the immediately preceding event of that
    passenger:

    * equal tap types (in->in or out->out) within the duplicate window give a
      ``duplicate_tap`` alert (a repeated action, not a closed trip);
    * an out->in transition that beats the directed station-pair minimum
      travel time gives a ``min_travel_violation`` alert.

    Returns the alerts in deterministic scan order: triggering timestamp,
    triggering arrival ``seq``, then rule name.
    """
    alerts: list[Alert] = []
    previous: Event | None = None
    for ev in events:
        if previous is not None:
            gap = (ev.occurred_at - previous.occurred_at).total_seconds()
            # Events are ordered, so the gap is non-negative.
            if previous.tap_type == ev.tap_type:
                # The duplicate bound is inclusive: a gap exactly equal to the
                # window still counts as a repeated same-type tap.
                if gap <= duplicate_window_seconds:
                    alerts.append(
                        Alert(
                            seq=0,
                            event_id=ev.event_id,
                            passenger_id=ev.passenger_id,
                            rule=DUPLICATE_RULE,
                            actual_interval_seconds=gap,
                            threshold_seconds=duplicate_window_seconds,
                            evidence=[_evidence_event(previous), _evidence_event(ev)],
                        )
                    )
            elif previous.tap_type == "out" and ev.tap_type == "in":
                minimum = (
                    min_travel_seconds.get(previous.station_id, {})
                    .get(ev.station_id)
                )
                if minimum is not None and gap < minimum:
                    # Strictly below the minimum: exactly meeting the minimum
                    # travel time is a legal tap-in.
                    alerts.append(
                        Alert(
                            seq=0,
                            event_id=ev.event_id,
                            passenger_id=ev.passenger_id,
                            rule=MIN_TRAVEL_RULE,
                            actual_interval_seconds=gap,
                            threshold_seconds=minimum,
                            evidence=[_evidence_event(previous), _evidence_event(ev)],
                        )
                    )
        previous = ev

    alerts.sort(key=lambda a: (
        a.evidence[-1]["occurred_at"],
        a.evidence[-1]["seq"],
        a.rule,
    ))
    for i, a in enumerate(alerts):
        a.seq = i
    return alerts


def load_window_events(
    conn,
    window_start: datetime,
    window_end: datetime,
    passengers: list[str] | None,
) -> dict[str, list[Event]]:
    """Load events inside the half-open window, grouped per passenger.

    The window filter and the ordering are evaluated in Python (not on the
    stored ISO strings): optional fractional seconds would break lexicographic
    comparisons at the boundary, mirroring the reconciliation snapshot.
    """
    sql = "SELECT * FROM events"
    params: tuple = ()
    if passengers is not None:
        placeholders = ",".join("?" for _ in passengers)
        sql += f" WHERE passenger_id IN ({placeholders})"
        params = tuple(passengers)
    sql += " ORDER BY passenger_id, occurred_at, seq"

    grouped: dict[str, list[Event]] = {}
    for r in conn.execute(sql, params).fetchall():
        occurred = normalize_ts(
            datetime.fromisoformat(r["occurred_at"].replace("Z", "+00:00"))
        )
        if not (window_start <= occurred < window_end):
            continue
        grouped.setdefault(r["passenger_id"], []).append(
            Event(
                event_id=r["event_id"],
                passenger_id=r["passenger_id"],
                station_id=r["station_id"],
                tap_type=r["tap_type"],
                occurred_at=occurred,
                seq=r["seq"],
            )
        )
    # The SQL ordering is lexicographic on stored ISO strings, which can drift
    # from chronological order when fractional seconds are present; the scan
    # rule requires true (occurred_at, seq, event_id) ordering.
    for pid_events in grouped.values():
        pid_events.sort(key=lambda e: (e.occurred_at, e.seq, e.event_id))
    return grouped


def canonical_scan_payload(
    scan_id: str,
    window_start: datetime,
    window_end: datetime,
    passengers: list[str] | None,
    duplicate_window_seconds: int,
    min_travel_seconds: dict[str, dict[str, int]],
) -> dict:
    """Canonical request: timestamps normalised to UTC, the passenger allow-list
    sorted and de-duplicated, and the station-pair map sorted, so semantically
    identical submissions hash identically regardless of array/object order or
    offset notation."""
    return {
        "scan_id": scan_id,
        "window_start": iso_str(window_start),
        "window_end": iso_str(window_end),
        "passengers": sorted(set(passengers)) if passengers is not None else None,
        "duplicate_window_seconds": duplicate_window_seconds,
        "min_travel_seconds": {
            out: {st: secs for st, secs in sorted(pairs.items())}
            for out, pairs in sorted(min_travel_seconds.items())
        },
    }


def payload_hash(payload: dict) -> str:
    return hashlib.sha256(
        canonical_json(payload).encode("utf-8")
    ).hexdigest()
