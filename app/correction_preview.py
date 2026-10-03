"""Correction impact preview (更正影响预览).

Given an existing ``event_id`` and a would-be replacement payload (passenger,
station, tap type, UTC time), this module runs the *official* settlement rules
purely in memory and reports what a real ``POST /events`` correction would do
to trips, amounts and the already booked, immutable downstream artefacts:

* reconciliation batches (frozen tap-out snapshots in ``recon_results``);
* clearing chains rooted at a first closing (frozen window + operator rules);
* resolved disputes with an immutable refund ledger row.

Nothing is ever written: no event, trip, charge, bill, audit record or any
append-only/frozen table is touched, and the idempotency/correction semantics
of ``POST /events`` are unchanged. When the passenger changes, both the old and
the new passenger are simulated. An identical payload previews as
``status="no_change"`` with empty adjustments and empty impacts.
"""
from __future__ import annotations

import json
from datetime import datetime

from .clearing import (
    last_seen_trips,
    plan_adjustment_entries,
    prior_operator_totals,
    rule_map_from_canonical,
)
from .config import FareConfig
from .settlement import Event, iso_str, normalize_ts, simulate_passenger


def _parse_iso(value: str) -> datetime:
    return normalize_ts(datetime.fromisoformat(value.replace("Z", "+00:00")))


def _event_from_row(row) -> Event:
    return Event(
        event_id=row["event_id"],
        passenger_id=row["passenger_id"],
        station_id=row["station_id"],
        tap_type=row["tap_type"],
        occurred_at=_parse_iso(row["occurred_at"]),
        seq=row["seq"],
    )


def _trip_view_from_trip(trip) -> dict:
    return {
        "trip_id": trip.trip_id,
        "passenger_id": trip.passenger_id,
        "from_station": trip.in_event.station_id,
        "to_station": trip.out_event.station_id,
        "from_zone": trip.from_zone,
        "to_zone": trip.to_zone,
        "started_at": iso_str(trip.in_event.occurred_at),
        "ended_at": iso_str(trip.out_event.occurred_at),
        "day_key": trip.day_key,
        "week_key": trip.week_key,
        "base_fare_cents": trip.base_fare_cents,
        "transfer_discount_cents": trip.transfer_discount_cents,
        "amount_cents": trip.amount_cents,
        "basis": trip.basis,
    }


def _stored_trip_views(conn, passenger_ids) -> dict[str, dict]:
    """Current persisted trips/charges for the given passengers, keyed by
    trip_id. This is exactly what GET /passengers/.../bill reads, so the
    preview's 'before' view matches the live bill."""
    if not passenger_ids:
        return {}
    marks = ",".join("?" for _ in passenger_ids)
    rows = conn.execute(
        "SELECT t.*, c.base_fare_cents, c.transfer_discount_cents,"
        " c.amount_cents AS charge_cents, c.basis"
        " FROM trips t JOIN trip_charges c ON c.trip_id = t.trip_id"
        f" WHERE t.passenger_id IN ({marks})"
        " ORDER BY t.started_at, t.trip_id",
        tuple(passenger_ids),
    ).fetchall()
    views: dict[str, dict] = {}
    for r in rows:
        views[r["trip_id"]] = {
            "trip_id": r["trip_id"],
            "passenger_id": r["passenger_id"],
            "from_station": r["from_station"],
            "to_station": r["to_station"],
            "from_zone": r["from_zone"],
            "to_zone": r["to_zone"],
            "started_at": r["started_at"],
            "ended_at": r["ended_at"],
            "day_key": r["day_key"],
            "week_key": r["week_key"],
            "base_fare_cents": r["base_fare_cents"],
            "transfer_discount_cents": r["transfer_discount_cents"],
            "amount_cents": r["charge_cents"],
            "basis": json.loads(r["basis"]),
        }
    return views


def _passenger_events(conn, passenger_id: str, replacement: Event) -> list[Event]:
    """Live non-held events for a passenger as they would look after the
    correction. The replacement keeps the event's original arrival ``seq`` --
    exactly what a real correction does (the seq column is never rewritten).

    When the passenger changes, the event is removed from the old passenger's
    history and added to the new passenger's history; for the old passenger it
    therefore simply disappears from pairing, mirroring ``POST /events`` which
    re-settles both passengers on a passenger change."""
    rows = conn.execute(
        "SELECT * FROM events WHERE passenger_id = ? AND held_id IS NULL"
        " ORDER BY occurred_at, seq",
        (passenger_id,),
    ).fetchall()
    events: list[Event] = []
    belongs_here = passenger_id == replacement.passenger_id
    swapped = False
    for r in rows:
        if r["event_id"] == replacement.event_id:
            if belongs_here:
                events.append(replacement)
                swapped = True
            # else: the event moves away from this passenger -- drop it.
        else:
            events.append(_event_from_row(r))
    if belongs_here and not swapped:
        # The event currently belongs to the other passenger: insert it into
        # this passenger's history under the same arrival seq.
        events.append(replacement)
    return events


def _trip_change_code(old: dict | None, new: dict | None) -> tuple[str, str]:
    """Classify how a single trip_id moved between the old and new views."""
    if old is None:
        return (
            "trip_appeared",
            (
                f"trip {new['trip_id']} appears after re-pairing"
                f" ({new['from_station']} -> {new['to_station']},"
                f" ends {new['ended_at']}, {new['amount_cents']} cents); it was"
                " not present before the simulated correction"
            ),
        )
    if new is None:
        return (
            "trip_voided",
            (
                f"trip {old['trip_id']} ({old['from_station']} ->"
                f" {old['to_station']}, {old['amount_cents']} cents) disappears"
                " after re-pairing"
            ),
        )
    reasons = []
    if old["passenger_id"] != new["passenger_id"]:
        reasons.append(
            f"passenger changes {old['passenger_id']} -> {new['passenger_id']}"
        )
    if (old["from_station"], old["to_station"]) != (
        new["from_station"],
        new["to_station"],
    ):
        reasons.append(
            "station pair changes "
            f"{old['from_station']}->{old['to_station']} -> "
            f"{new['from_station']}->{new['to_station']}"
        )
    if old["ended_at"] != new["ended_at"]:
        reasons.append(f"tap-out time changes {old['ended_at']} -> {new['ended_at']}")
    elif old["started_at"] != new["started_at"]:
        reasons.append(
            f"tap-in time changes {old['started_at']} -> {new['started_at']}"
        )
    if old["amount_cents"] != new["amount_cents"]:
        reasons.append(
            f"amount changes {old['amount_cents']} -> {new['amount_cents']} cents"
        )
    if not reasons:
        return ("unchanged", f"trip {old['trip_id']} is unchanged")
    if old["passenger_id"] != new["passenger_id"]:
        code = "passenger_changed"
    elif (old["from_station"], old["to_station"]) != (
        new["from_station"],
        new["to_station"],
    ):
        code = "stations_changed"
    elif old["ended_at"] != new["ended_at"] or old["started_at"] != new["started_at"]:
        code = "timing_changed"
    else:
        code = "amount_changed"
    return code, "; ".join(reasons)


def no_change_report(conn, passenger_id: str) -> dict:
    """The report for a payload identical to the stored event: the passenger's
    current trips are echoed on both sides with no adjustments or impacts."""
    views = _stored_trip_views(conn, [passenger_id])
    trips = [views[tid] for tid in sorted(views)]
    total = sum(t["amount_cents"] for t in trips)
    return {
        "simulated_passengers": [passenger_id],
        "passengers": [{
            "passenger_id": passenger_id,
            "old_trips": trips,
            "new_trips": [dict(t) for t in trips],
            "adjustments": [],
            "old_total_cents": total,
            "new_total_cents": total,
            "delta_cents": 0,
        }],
        "total_delta_cents": 0,
        "reconciliation_impacts": [],
        "clearing_impacts": [],
        "resolved_dispute_impacts": [],
    }


def simulate_correction(
    conn,
    cfg: FareConfig,
    existing_row,
    replacement: Event,
) -> dict:
    """Run the whole correction simulation against the current snapshot.

    ``existing_row`` is the live events row for the event_id; ``replacement``
    is the hypothetical event (same event_id, new payload). Returns a dict with
    per-passenger before/after trip views and adjustments, plus the impact
    lists for reconciliation batches, clearing chains and resolved disputes.
    Purely read-only.
    """
    old_passenger = existing_row["passenger_id"]
    new_passenger = replacement.passenger_id
    passengers = sorted({old_passenger, new_passenger})

    old_views = _stored_trip_views(conn, passengers)
    old_views_by_pid: dict[str, dict[str, dict]] = {pid: {} for pid in passengers}
    for tid, view in old_views.items():
        old_views_by_pid[view["passenger_id"]][tid] = view
    old_charges_by_pid: dict[str, dict[str, int]] = {
        pid: {tid: v["amount_cents"] for tid, v in views.items()}
        for pid, views in old_views_by_pid.items()
    }

    passenger_blocks: list[dict] = []
    new_views: dict[str, dict] = {}
    for pid in passengers:
        events = _passenger_events(conn, pid, replacement)
        trips, _pending, adjustments = simulate_passenger(
            events, old_charges_by_pid[pid], cfg
        )
        views = {t.trip_id: _trip_view_from_trip(t) for t in trips}
        new_views.update(views)
        ordered_old = sorted(
            old_views_by_pid[pid].values(),
            key=lambda v: (v["started_at"], v["trip_id"]),
        )
        ordered_new = [views[t.trip_id] for t in trips]
        old_total = sum(v["amount_cents"] for v in ordered_old)
        new_total = sum(v["amount_cents"] for v in ordered_new)
        passenger_blocks.append({
            "passenger_id": pid,
            "old_trips": ordered_old,
            "new_trips": ordered_new,
            "adjustments": adjustments,
            "old_total_cents": old_total,
            "new_total_cents": new_total,
            "delta_cents": new_total - old_total,
        })

    all_ids = set(old_views) | set(new_views)
    changed: dict[str, tuple[str, str]] = {}
    for tid in all_ids:
        code, reason = _trip_change_code(old_views.get(tid), new_views.get(tid))
        if code != "unchanged":
            changed[tid] = (code, reason)
    appeared = {tid for tid, (code, _r) in changed.items() if code == "trip_appeared"}

    recon_impacts = _reconciliation_impacts(
        conn, changed, old_views, new_views, appeared
    )
    clearing_impacts = _clearing_impacts(conn, changed, new_views, old_views)
    dispute_impacts = _dispute_impacts(conn, changed, old_views, new_views)

    total_delta = sum(b["delta_cents"] for b in passenger_blocks)
    return {
        "simulated_passengers": passengers,
        "passengers": passenger_blocks,
        "total_delta_cents": total_delta,
        "reconciliation_impacts": recon_impacts,
        "clearing_impacts": clearing_impacts,
        "resolved_dispute_impacts": dispute_impacts,
    }


# ---------------------------------------------------------------------------
# Reconciliation batches
# ---------------------------------------------------------------------------


def _reconciliation_impacts(conn, changed, old_views, new_views, appeared) -> list[dict]:
    """Every frozen recon result row that references a changed trip_id, plus
    entries for changed trips that end inside a batch's frozen window although
    the batch never snapshotted them (they pair in or move into the window as a
    result of the simulated correction). Read-only."""
    impacts: list[dict] = []
    batches = conn.execute(
        "SELECT batch_id, window_start, window_end FROM recon_batches"
        " ORDER BY batch_id"
    ).fetchall()
    for batch in batches:
        window_start = _parse_iso(batch["window_start"])
        window_end = _parse_iso(batch["window_end"])
        rows = conn.execute(
            "SELECT seq, category, passenger_id, trip_id, serial_no, match_method"
            " FROM recon_results WHERE batch_id = ? ORDER BY seq",
            (batch["batch_id"],),
        ).fetchall()
        seen_trips: set[str] = set()
        for r in rows:
            tid = r["trip_id"]
            if tid is not None:
                seen_trips.add(tid)
            if tid is None or tid not in changed:
                continue
            code, reason = changed[tid]
            impacts.append({
                "batch_id": batch["batch_id"],
                "trip_id": tid,
                "serial_no": r["serial_no"],
                "passenger_id": r["passenger_id"],
                "category": r["category"],
                "match_method": r["match_method"],
                "change_code": code,
                "reason": (
                    f"frozen reconciliation result (category {r['category']},"
                    f" method {r['match_method']}) is stale: {reason}"
                ),
            })
        # Changed trips the batch never snapshotted but that end inside its
        # frozen window after the correction -- either because re-pairing
        # creates them (trip_appeared) or because a timing/passenger change
        # moves an existing trip into the window. The stored batch cannot
        # account for either; a new batch would be needed.
        for tid in sorted(changed):
            if tid in seen_trips:
                continue
            view = new_views.get(tid)
            if view is None:
                continue
            ended = _parse_iso(view["ended_at"])
            if not (window_start <= ended < window_end):
                continue
            old_view = old_views.get(tid)
            if old_view is not None:
                old_ended = _parse_iso(old_view["ended_at"])
                if window_start <= old_ended < window_end:
                    # It was already inside the window; it simply was not part
                    # of this batch's snapshot (e.g. another passenger). Only
                    # genuinely pairing/moving into the window is reported.
                    continue
                move_phrase = "moves into the frozen window after the simulated time/passenger change"
            else:
                move_phrase = "appears through re-pairing and ends inside the frozen window"
            impacts.append({
                "batch_id": batch["batch_id"],
                "trip_id": tid,
                "serial_no": None,
                "passenger_id": view["passenger_id"],
                "category": None,
                "match_method": None,
                "change_code": "trip_appeared",
                "reason": (
                    f"trip {tid} ({view['from_station']} -> {view['to_station']},"
                    f" {view['amount_cents']} cents) {move_phrase} at"
                    f" {view['ended_at']}; it was not part of batch"
                    f" '{batch['batch_id']}' snapshot, so the stored batch"
                    " cannot account for it"
                ),
            })
    impacts.sort(key=lambda i: (i["batch_id"], i["trip_id"] or "", i["serial_no"] or ""))
    return impacts


# ---------------------------------------------------------------------------
# Clearing chains
# ---------------------------------------------------------------------------


def _hypothetical_window_trips(conn, window_start, window_end, new_views) -> list[dict]:
    """The equivalent of ``load_clearing_trips`` for the simulated snapshot:
    current in-window trips, with simulated passengers' trips replaced by the
    preview's new views. Trips of passengers not touched by the simulation are
    read straight from storage."""
    simulated_ids = set(new_views)
    trips: list[dict] = []
    rows = conn.execute(
        "SELECT t.trip_id, t.passenger_id, t.from_station, t.to_station,"
        " t.ended_at, c.amount_cents"
        " FROM trips t JOIN trip_charges c ON c.trip_id = t.trip_id"
    ).fetchall()
    for r in rows:
        if r["trip_id"] in simulated_ids:
            continue
        ended = _parse_iso(r["ended_at"])
        if window_start <= ended < window_end:
            trips.append({
                "trip_id": r["trip_id"],
                "passenger_id": r["passenger_id"],
                "from_station": r["from_station"],
                "to_station": r["to_station"],
                "ended": ended,
                "ended_at": iso_str(ended),
                "amount_cents": r["amount_cents"],
            })
    for view in new_views.values():
        ended = _parse_iso(view["ended_at"])
        if window_start <= ended < window_end:
            trips.append({
                "trip_id": view["trip_id"],
                "passenger_id": view["passenger_id"],
                "from_station": view["from_station"],
                "to_station": view["to_station"],
                "ended": ended,
                "ended_at": iso_str(ended),
                "amount_cents": view["amount_cents"],
            })
    trips.sort(key=lambda t: (t["ended"], t["trip_id"]))
    return trips


def _chain_may_change(
    seen_before: dict[str, dict],
    hypothetical_ids: set[str],
    changed_ids: set[str],
) -> bool:
    """Only chains that booked a trip the correction touches, or whose frozen
    window would newly contain such a trip, are reported."""
    for tid in seen_before:
        if tid in changed_ids:
            return True
    for tid in hypothetical_ids:
        if tid in changed_ids:
            return True
    return False


def _clearing_impacts(conn, changed, new_views, old_views) -> list[dict]:
    impacts: list[dict] = []
    roots = conn.execute(
        "SELECT * FROM clearing_batches WHERE kind = 'initial' AND seq = 0"
        " ORDER BY clearing_id"
    ).fetchall()
    for root in roots:
        window_start = _parse_iso(root["window_start"])
        window_end = _parse_iso(root["window_end"])
        next_seq = conn.execute(
            "SELECT COALESCE(MAX(seq), 0) + 1 FROM clearing_batches"
            " WHERE root_clearing_id = ?",
            (root["clearing_id"],),
        ).fetchone()[0]
        prior = prior_operator_totals(conn, root["clearing_id"], next_seq)
        seen_before = last_seen_trips(conn, root["clearing_id"], next_seq)
        hypothetical = _hypothetical_window_trips(
            conn, window_start, window_end, new_views
        )
        hypothetical_by_id = {t["trip_id"]: t for t in hypothetical}
        if not _chain_may_change(
            seen_before, set(hypothetical_by_id), set(changed)
        ):
            continue

        rules = json.loads(root["rules"])
        rules_by_pair = rule_map_from_canonical(rules)
        plan = plan_adjustment_entries(
            rules_by_pair,
            hypothetical,
            prior,
            seen_before,
            trip_filter=set(changed),
        )

        trip_entries: dict[str, list[dict]] = {}
        for e in plan["entries"]:
            trip_entries.setdefault(e["trip_id"], []).append(e)

        # Cover missing-rule gaps attributable to a simulated trip: those are
        # exactly the pairs the frozen rules cannot book after the correction.
        simulated_missing = sorted({
            f"{t['from_station']}->{t['to_station']}"
            for t in hypothetical
            if t["trip_id"] in changed
            and (t["from_station"], t["to_station"]) not in rules_by_pair
        })

        trip_reports: list[dict] = []
        reported_trips: set[str] = set(trip_entries)
        # Structural changes (trip appearing / moving station pairs / changing
        # passenger or timing) are reported even when the net amount the next
        # adjustment batch would book is zero. Amount-only changes with a zero
        # delta book nothing, so they are skipped; a trip already reversed to
        # zero by an earlier adjustment batch has nothing left to reverse.
        for tid in sorted(changed):
            if tid in reported_trips:
                continue
            new_t = hypothetical_by_id.get(tid)
            old_seen = seen_before.get(tid)
            if new_t is None and old_seen is None:
                continue
            code, _generic_reason = changed[tid]
            if code == "amount_changed":
                continue
            if new_t is None and old_seen is not None:
                continue
            trip_reports.append(_clearing_trip_report(
                tid, changed[tid], old_views.get(tid), new_views.get(tid),
                old_seen, new_t, [],
            ))
            reported_trips.add(tid)

        for tid in sorted(trip_entries):
            entries = trip_entries[tid]
            trip_reports.append(_clearing_trip_report(
                tid,
                changed.get(tid, ("amount_changed", "")),
                old_views.get(tid),
                new_views.get(tid),
                seen_before.get(tid),
                hypothetical_by_id.get(tid),
                entries,
            ))

        if not trip_reports and not simulated_missing:
            continue

        net_delta = sum(e["amount_cents"] for e in plan["entries"])
        trip_reports.sort(key=lambda t: (t.get("_ended") or "", t["trip_id"] or ""))
        for t in trip_reports:
            t.pop("_ended", None)
        impacts.append({
            "base_clearing_id": root["clearing_id"],
            "window_start": root["window_start"],
            "window_end": root["window_end"],
            "trips": trip_reports,
            "delta_cents": net_delta,
            "rule_coverage": "uncovered" if simulated_missing else "covered",
            "uncovered_station_pairs": simulated_missing,
        })
    impacts.sort(key=lambda i: i["base_clearing_id"])
    return impacts


def _clearing_trip_report(
    trip_id, change, old_view, new_view, old_seen, new_trip, entries
) -> dict:
    code, generic_reason = change
    old_from = (old_seen or old_view or {}).get("from_station")
    old_to = (old_seen or old_view or {}).get("to_station")
    new_from = (new_trip or new_view or {}).get("from_station")
    new_to = (new_trip or new_view or {}).get("to_station")
    old_gross = (
        old_seen["gross_amount_cents"] if old_seen is not None
        else (old_view["amount_cents"] if old_view is not None else 0)
    )
    new_gross = new_trip["amount_cents"] if new_trip is not None else 0

    operators = []
    delta_total = 0
    for e in entries:
        operators.append({
            "operator_id": e["operator_id"],
            "booked_cents": e["prior_amount_cents"],
            "target_cents": e["cumulative_amount_cents"],
            "delta_cents": e["amount_cents"],
        })
        delta_total += e["amount_cents"]
    if code == "trip_voided":
        op_phrase = "; ".join(
            f"{o['operator_id']} {o['delta_cents']}" for o in operators
        )
        reason = (
            f"trip {trip_id} vanishes in the frozen window; the next adjustment"
            f" batch reverses the booked {old_gross} cents"
            + (f" ({op_phrase})" if op_phrase else "")
        )
    elif (old_from, old_to) != (new_from, new_to) and new_trip is not None:
        reason = (
            f"station pair changes {old_from}->{old_to} -> {new_from}->{new_to};"
            " the next adjustment batch reverses operators unique to the old"
            " rule and books operators under the new rule"
        )
    elif code == "trip_appeared":
        reason = (
            f"trip {trip_id} newly ends inside the frozen window"
            f" ({new_from}->{new_to}, gross {new_gross}); the next adjustment"
            " batch would book it under the frozen rules"
        )
    else:
        reason = generic_reason or (
            f"trip {trip_id} amount changes; the next adjustment batch books the"
            " target-minus-cumulative delta"
        )
    return {
        "trip_id": trip_id,
        "change_code": code,
        "reason": reason,
        "old_from_station": old_from,
        "old_to_station": old_to,
        "new_from_station": new_from,
        "new_to_station": new_to,
        "old_gross_cents": old_gross,
        "new_gross_cents": new_gross,
        "delta_cents": delta_total,
        "operators": operators,
        "_ended": (new_trip or old_seen or {}).get("ended_at"),
    }


# ---------------------------------------------------------------------------
# Resolved disputes
# ---------------------------------------------------------------------------


def _direction(difference: int) -> str:
    if difference > 0:
        return "top_up_due"
    if difference < 0:
        return "clawback_due"
    return "none"


def _dispute_impacts(conn, changed, old_views, new_views) -> list[dict]:
    """Resolved disputes (an immutable refund row exists) whose disputed trip
    is touched by the simulated correction. Open disputes are not booked
    artefacts and are intentionally not listed."""
    rows = conn.execute(
        "SELECT d.dispute_id, d.passenger_id, d.trip_id, s.amount_cents AS snap,"
        " r.amount_cents AS refund"
        " FROM refunds r"
        " JOIN disputes d ON d.dispute_id = r.dispute_id"
        " JOIN dispute_snapshots s ON s.dispute_id = d.dispute_id"
        " ORDER BY d.dispute_id"
    ).fetchall()
    impacts: list[dict] = []
    for r in rows:
        tid = r["trip_id"]
        if tid not in changed:
            continue
        code, generic_reason = changed[tid]
        old_live = old_views.get(tid)
        new_live = new_views.get(tid)
        old_current = old_live["amount_cents"] if old_live is not None else 0
        new_current = new_live["amount_cents"] if new_live is not None else 0
        net_paid = r["snap"] - r["refund"]
        old_diff = net_paid - old_current
        new_diff = net_paid - new_current
        if code == "trip_voided":
            reason = (
                f"disputed trip {tid} disappears after re-pairing; the settled"
                f" net payment {net_paid} cents would be fully reportable as a"
                " top-up while the immutable refund stays unchanged"
            )
        elif code == "passenger_changed":
            reason = (
                f"disputed trip {tid} moves to another passenger after"
                " re-pairing; the resolved dispute's frozen snapshot and refund"
                " stay unchanged but the live comparison trip changes identity"
            )
        else:
            reason = (
                f"resolved dispute references a changed trip ({generic_reason});"
                " the frozen snapshot and immutable refund stay unchanged and"
                " only the reported top-up/clawback difference moves"
            )
        impacts.append({
            "dispute_id": r["dispute_id"],
            "passenger_id": r["passenger_id"],
            "trip_id": tid,
            "state": "resolved",
            "change_code": code,
            "reason": reason,
            "frozen_amount_cents": r["snap"],
            "refunded_cents": r["refund"],
            "net_paid_cents": net_paid,
            "old_current_cents": old_current,
            "new_current_cents": new_current,
            "old_difference_cents": old_diff,
            "new_difference_cents": new_diff,
            "old_direction": _direction(old_diff),
            "new_direction": _direction(new_diff),
        })
    return impacts
