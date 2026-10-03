"""Correction impact preview (更正影响预览).

``POST /events/correction-preview`` is the read-only dry-run of the
``POST /events`` correction path. The caller submits an existing ``event_id``
plus the replacement payload (passenger, station, tap type and UTC time); the
service replays the formal settlement rules in memory for every affected
passenger -- the event's current owner and, when the passenger changes, the
replacement passenger as well -- and reports exactly what the real correction
would produce: per-trip old/new amounts with their billing basis, and the
projected pairing status of the corrected event.

Nothing is persisted: events, trips, charges, the audit chain and every
frozen table are only read, never written, and no audit record is appended.
On top of the settlement diff the preview itemises the already-booked
artefacts the correction would leave stale -- reconciliation batches,
clearing chains and resolved disputes -- with one row per
(artefact, trip, cause) and a human-readable reason.
"""
from __future__ import annotations

import json
from datetime import datetime

from .config import FareConfig
from .settlement import (
    Event,
    compute_adjustments,
    load_events,
    pair_trips,
    price_trips,
)

# Causes reported in an impact row.
CAUSE_TRIP_VANISHED = "trip_vanished"
CAUSE_AMOUNT_CHANGED = "amount_changed"
CAUSE_STATION_CHANGED = "station_changed"


def simulate_correction(
    conn,
    cfg: FareConfig,
    event_row,
    new_passenger_id: str,
    new_station_id: str,
    new_tap_type: str,
    new_occurred: datetime,
) -> dict:
    """Dry-run the formal settlement for a payload replacement.

    The corrected event keeps its original arrival ``seq`` -- exactly what the
    ``POST /events`` UPDATE leaves behind -- so the simulated pairing order is
    identical to the one the real correction would produce. Returns the
    per-passenger simulations, the projected pairing status of the corrected
    event, and the simulated after-state of every affected passenger's trips.
    """
    old_passenger = event_row["passenger_id"]
    moved = Event(
        event_id=event_row["event_id"],
        passenger_id=new_passenger_id,
        station_id=new_station_id,
        tap_type=new_tap_type,
        occurred_at=new_occurred,
        seq=event_row["seq"],
    )

    simulations: list[dict] = []
    after_trips: dict[str, dict] = {}
    event_pairing = None
    pending_reason = None
    for pid in sorted({old_passenger, new_passenger_id}):
        events = [e for e in load_events(conn, pid) if e.event_id != moved.event_id]
        if pid == new_passenger_id:
            events.append(moved)
        trips, pending = pair_trips(events)
        price_trips(trips, cfg)
        old_charges = {
            r["trip_id"]: r["amount_cents"]
            for r in conn.execute(
                "SELECT trip_id, amount_cents FROM trip_charges WHERE passenger_id = ?",
                (pid,),
            )
        }
        simulations.append(
            {
                "passenger_id": pid,
                "trip_count_before": len(old_charges),
                "trip_count_after": len(trips),
                "adjustments": compute_adjustments(trips, old_charges),
            }
        )
        for t in trips:
            after_trips[t.trip_id] = {
                "passenger_id": pid,
                "amount_cents": t.amount_cents,
                "from_station": t.in_event.station_id,
                "to_station": t.out_event.station_id,
            }
        if pid == new_passenger_id:
            paired_ids = {t.in_event.event_id for t in trips} | {
                t.out_event.event_id for t in trips
            }
            if moved.event_id in paired_ids:
                event_pairing, pending_reason = "paired", None
            else:
                reason = {p.event.event_id: p.reason for p in pending}[moved.event_id]
                event_pairing, pending_reason = "pending", reason

    return {
        "simulations": simulations,
        "after_trips": after_trips,
        "event_pairing": event_pairing,
        "pending_reason": pending_reason,
    }


def _trip_changes(conn, passenger_ids, after_trips: dict) -> dict:
    """Before/after descriptors for every trip of the simulated passengers,
    keyed by trip_id. ``before`` is the currently persisted state (None when
    the trip does not exist yet); ``after`` is the simulated state (None when
    the correction would void the trip)."""
    changes: dict[str, dict] = {}
    for pid in passenger_ids:
        rows = conn.execute(
            "SELECT t.trip_id, t.from_station, t.to_station, c.amount_cents"
            " FROM trips t JOIN trip_charges c ON c.trip_id = t.trip_id"
            " WHERE t.passenger_id = ?",
            (pid,),
        ).fetchall()
        for r in rows:
            changes[r["trip_id"]] = {
                "passenger_id": pid,
                "before": {
                    "amount_cents": r["amount_cents"],
                    "from_station": r["from_station"],
                    "to_station": r["to_station"],
                },
                "after": None,
            }
    for trip_id, desc in after_trips.items():
        entry = changes.setdefault(
            trip_id,
            {"passenger_id": desc["passenger_id"], "before": None, "after": None},
        )
        entry["after"] = desc
    return changes


def _causes(change: dict) -> list[str]:
    """Why a booked artefact referencing this trip would go stale."""
    before, after = change["before"], change["after"]
    if before is not None and after is None:
        return [CAUSE_TRIP_VANISHED]
    causes = []
    old_amount = before["amount_cents"] if before else 0
    new_amount = after["amount_cents"] if after else 0
    if old_amount != new_amount:
        causes.append(CAUSE_AMOUNT_CHANGED)
    if (
        before is not None
        and after is not None
        and (before["from_station"], before["to_station"])
        != (after["from_station"], after["to_station"])
    ):
        causes.append(CAUSE_STATION_CHANGED)
    return causes


def _amount(desc: dict | None) -> int:
    return desc["amount_cents"] if desc else 0


def _pair(desc: dict) -> str:
    return f"{desc['from_station']}->{desc['to_station']}"


def build_impacts(conn, changes: dict) -> list[dict]:
    """Itemise the booked artefacts (reconciliation batches, clearing chains,
    resolved disputes) that reference a trip the correction would change.
    Open disputes are not booked artefacts and are never listed."""
    impacts: list[dict] = []

    def add(artefact: str, artefact_id: str, trip_id: str, change: dict,
            cause: str, reason: str) -> None:
        impacts.append(
            {
                "artefact": artefact,
                "artefact_id": artefact_id,
                "passenger_id": change["passenger_id"],
                "trip_id": trip_id,
                "cause": cause,
                "old_amount_cents": _amount(change["before"]),
                "new_amount_cents": _amount(change["after"]),
                "reason": reason,
            }
        )

    # --- Reconciliation batches: frozen result rows reference trip ids. ---
    rows = conn.execute(
        "SELECT batch_id, trip_id, internal_amount_cents FROM recon_results"
        " WHERE trip_id IS NOT NULL ORDER BY batch_id, seq"
    ).fetchall()
    for r in rows:
        change = changes.get(r["trip_id"])
        if change is None:
            continue
        before, after = change["before"], change["after"]
        booked = r["internal_amount_cents"]
        for cause in _causes(change):
            if cause == CAUSE_TRIP_VANISHED:
                reason = (
                    f"the correction would void this trip; batch '{r['batch_id']}'"
                    f" froze a result row for it (booked internal amount {booked}"
                    " cents) that a new snapshot can no longer reproduce"
                )
            elif cause == CAUSE_AMOUNT_CHANGED:
                reason = (
                    f"trip amount would change {_amount(before)} ->"
                    f" {_amount(after)} cents; batch '{r['batch_id']}' froze"
                    f" internal amount {booked} cents for it"
                )
            else:
                reason = (
                    f"trip stations would change {_pair(before)} to {_pair(after)};"
                    f" batch '{r['batch_id']}' froze its result against the old"
                    " stations"
                )
            add("reconciliation", r["batch_id"], r["trip_id"], change, cause, reason)

    # --- Clearing chains: immutable per-trip/operator ledger rows. ---
    rules_by_root = {
        r["clearing_id"]: json.loads(r["rules"])
        for r in conn.execute(
            "SELECT clearing_id, rules FROM clearing_batches WHERE seq = 0"
        )
    }
    rows = conn.execute(
        "SELECT b.root_clearing_id AS root_id, e.trip_id,"
        " SUM(e.amount_cents) AS booked_cents"
        " FROM clearing_entries e"
        " JOIN clearing_batches b ON b.clearing_id = e.clearing_id"
        " GROUP BY b.root_clearing_id, e.trip_id"
    ).fetchall()
    for r in rows:
        change = changes.get(r["trip_id"])
        if change is None:
            continue
        before, after = change["before"], change["after"]
        booked = r["booked_cents"]
        root = r["root_id"]
        for cause in _causes(change):
            if cause == CAUSE_TRIP_VANISHED:
                reason = (
                    f"the correction would void this trip; the next adjustment on"
                    f" chain '{root}' would reverse the {booked} cents booked for"
                    " it back to zero"
                )
            elif cause == CAUSE_AMOUNT_CHANGED:
                reason = (
                    f"trip gross would change {_amount(before)} ->"
                    f" {_amount(after)} cents; the next adjustment on chain"
                    f" '{root}' would book the per-operator delta against the"
                    f" {booked} cents already booked"
                )
            else:
                new_pair = _pair(after)
                covered = any(
                    rule["from_station"] == after["from_station"]
                    and rule["to_station"] == after["to_station"]
                    for rule in rules_by_root.get(root, [])
                )
                if covered:
                    reason = (
                        f"trip station pair would change {_pair(before)} to"
                        f" {new_pair}; the next adjustment on chain '{root}'"
                        f" would re-settle it under the frozen rule for"
                        f" '{new_pair}'"
                    )
                else:
                    reason = (
                        f"trip station pair would change {_pair(before)} to"
                        f" {new_pair}, which the frozen rules of chain '{root}'"
                        " do not cover; the next adjustment batch would be"
                        " rejected (422)"
                    )
            add("clearing", root, r["trip_id"], change, cause, reason)

    # --- Resolved disputes: the immutable refund was settled against the
    # frozen snapshot; the live amount drives the reported difference. ---
    rows = conn.execute(
        "SELECT d.dispute_id, d.trip_id, s.amount_cents AS snapshot_cents,"
        " r.amount_cents AS refund_cents"
        " FROM disputes d"
        " JOIN dispute_snapshots s ON s.dispute_id = d.dispute_id"
        " JOIN refunds r ON r.dispute_id = d.dispute_id"
        " ORDER BY d.dispute_id"
    ).fetchall()
    for r in rows:
        change = changes.get(r["trip_id"])
        if change is None:
            continue
        before, after = change["before"], change["after"]
        settled = r["snapshot_cents"] - r["refund_cents"]
        new_live = _amount(after)
        difference = settled - new_live
        if difference > 0:
            direction = f"top_up_due {difference} cents"
        elif difference < 0:
            direction = f"clawback_due {-difference} cents"
        else:
            direction = "none"
        for cause in _causes(change):
            if cause == CAUSE_TRIP_VANISHED:
                reason = (
                    f"the correction would void this trip (live amount 0);"
                    f" resolved dispute '{r['dispute_id']}' settled at {settled}"
                    f" cents net, so its reported difference would become"
                    f" {direction}"
                )
            elif cause == CAUSE_AMOUNT_CHANGED:
                reason = (
                    f"live amount would change {_amount(before)} -> {new_live}"
                    f" cents; resolved dispute '{r['dispute_id']}' settled at"
                    f" {settled} cents net, so its reported difference would"
                    f" become {direction}"
                )
            else:
                reason = (
                    f"trip stations would change {_pair(before)} to"
                    f" {_pair(after)}; resolved dispute '{r['dispute_id']}' keeps"
                    " its frozen snapshot and refund, and its reported difference"
                    f" would be {direction}"
                )
            add("dispute", r["dispute_id"], r["trip_id"], change, cause, reason)

    impacts.sort(
        key=lambda i: (i["artefact"], i["artefact_id"], i["trip_id"], i["cause"])
    )
    return impacts


def build_correction_preview(
    conn,
    cfg: FareConfig,
    event_row,
    new_passenger_id: str,
    new_station_id: str,
    new_tap_type: str,
    new_occurred: datetime,
) -> dict:
    """Full preview for a payload replacement on an existing event: the
    per-passenger settlement simulation plus the booked-artefact impacts."""
    result = simulate_correction(
        conn, cfg, event_row,
        new_passenger_id, new_station_id, new_tap_type, new_occurred,
    )
    changes = _trip_changes(
        conn,
        [s["passenger_id"] for s in result["simulations"]],
        result["after_trips"],
    )
    result["impacts"] = build_impacts(conn, changes)
    return result
