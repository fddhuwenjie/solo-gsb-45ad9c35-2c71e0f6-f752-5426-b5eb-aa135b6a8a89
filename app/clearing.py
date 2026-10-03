"""Operator fare clearing (运营方票款清分).

A *first closing* (``POST /clearings``) freezes the half-open UTC settlement
window (on trip tap-out time ``ended_at``) and the per-station-pair operator
rules, reads every currently paired trip charge in the window, splits each
integer-cent amount by basis points (万分比, 10000 = 100%) and writes immutable
per-trip / per-operator ledger rows.

Indivisible cents left after floor division are handed out one each in
descending fractional remainder order; exact remainder ties are broken by
operator id ascending, so every split is total-preserving and deterministic.

*Adjustment batches* (``POST /clearings/adjustments``) always reference the
first closing: the window and rules stay frozen on the root batch. For each
trip currently in the window the target per-operator amount is split from the
*current* charge, the cumulative amount already booked for that
trip/operator across every earlier batch of the chain is subtracted, and only
the signed difference is appended. Trips that have disappeared (or left the
frozen window) are reversed to zero; trips whose station pair changed are
settled against the new pair, so old operators receive negative reversals and
new operators positive top-ups. Because each batch recomputes
``target - cumulative`` from scratch, repeating an adjustment (as an
idempotent replay or under a fresh batch id) books nothing twice.
"""
from __future__ import annotations

import hashlib
from datetime import datetime

from .audit import canonical_json
from .settlement import iso_str, normalize_ts

TOTAL_BASIS_POINTS = 10000


def validate_rules(rules) -> str | None:
    """Structural rule validation. Returns a human-readable problem string, or
    None when every station pair has a single rule whose positive operator
    shares sum to exactly 10000 basis points."""
    seen: set[tuple[str, str]] = set()
    for rule in rules:
        pair = (rule.from_station, rule.to_station)
        if pair in seen:
            return f"duplicate operator rule for station pair {pair[0]!r} -> {pair[1]!r}"
        seen.add(pair)
        if not rule.shares:
            return (
                f"rule for station pair {pair[0]!r} -> {pair[1]!r} must list at"
                " least one operator share"
            )
        operators: set[str] = set()
        total = 0
        for share in rule.shares:
            if share.operator_id in operators:
                return (
                    f"duplicate operator_id {share.operator_id!r} in rule for"
                    f" station pair {pair[0]!r} -> {pair[1]!r}"
                )
            operators.add(share.operator_id)
            if share.basis_points < 0 or share.basis_points > TOTAL_BASIS_POINTS:
                return (
                    f"basis_points for operator {share.operator_id!r} must be in"
                    f" [0, {TOTAL_BASIS_POINTS}]"
                )
            total += share.basis_points
        if total != TOTAL_BASIS_POINTS:
            return (
                f"shares for station pair {pair[0]!r} -> {pair[1]!r} sum to"
                f" {total}, must sum to {TOTAL_BASIS_POINTS}"
            )
    return None


def canonical_rules(rules) -> list[dict]:
    """Canonical rule set: pairs sorted by (from, to) and each rule's shares
    sorted by operator id, so semantically identical submissions hash
    identically regardless of array order."""
    return sorted(
        (
            {
                "from_station": rule.from_station,
                "to_station": rule.to_station,
                "shares": sorted(
                    (
                        {
                            "operator_id": share.operator_id,
                            "basis_points": share.basis_points,
                        }
                        for share in rule.shares
                    ),
                    key=lambda s: s["operator_id"],
                ),
            }
            for rule in rules
        ),
        key=lambda r: (r["from_station"], r["to_station"]),
    )


def rule_map_from_canonical(rules: list[dict]) -> dict[tuple[str, str], list[tuple[str, int]]]:
    return {
        (rule["from_station"], rule["to_station"]): [
            (share["operator_id"], share["basis_points"]) for share in rule["shares"]
        ]
        for rule in rules
    }


def canonical_initial_payload(
    clearing_id: str,
    window_start: datetime,
    window_end: datetime,
    rules: list[dict],
) -> dict:
    return {
        "kind": "initial",
        "clearing_id": clearing_id,
        "window_start": iso_str(window_start),
        "window_end": iso_str(window_end),
        "rules": rules,
    }


def canonical_adjustment_payload(clearing_id: str, base_clearing_id: str) -> dict:
    # Adjustment batches carry no window or rules of their own: both stay
    # frozen on the referenced first closing, so the base reference is the
    # whole payload.
    return {
        "kind": "adjustment",
        "clearing_id": clearing_id,
        "base_clearing_id": base_clearing_id,
    }


def payload_hash(payload: dict) -> str:
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def split_amount(
    amount_cents: int, shares: list[tuple[str, int]]
) -> dict[str, int]:
    """Split an integer-cent amount by basis points (10000 = 100%).

    Each operator receives ``floor(amount * bp / 10000)``; the leftover cents
    are distributed one each in descending fractional-remainder order, with
    exact ties broken by operator id ascending. The allocations always sum
    exactly to ``amount_cents`` (which may be 0 for a capped trip).
    """
    bases: dict[str, int] = {}
    remainders: dict[str, int] = {}
    for operator_id, basis_points in shares:
        scaled = amount_cents * basis_points
        bases[operator_id] = scaled // TOTAL_BASIS_POINTS
        remainders[operator_id] = scaled % TOTAL_BASIS_POINTS

    leftover = amount_cents - sum(bases.values())
    order = sorted(shares, key=lambda s: (-remainders[s[0]], s[0]))
    allocations = dict(bases)
    for i in range(leftover):
        allocations[order[i][0]] += 1
    return allocations


def load_clearing_trips(conn, window_start: datetime, window_end: datetime) -> list[dict]:
    """Current paired trip charges whose tap-out time falls in the half-open
    window ``[window_start, window_end)``. Filtering uses parsed datetimes
    because stored ISO strings may carry optional fractional seconds, which
    breaks lexicographic comparison at the boundary."""
    rows = conn.execute(
        "SELECT t.trip_id, t.passenger_id, t.from_station, t.to_station,"
        " t.ended_at, c.amount_cents"
        " FROM trips t JOIN trip_charges c ON c.trip_id = t.trip_id"
    ).fetchall()
    trips: list[dict] = []
    for r in rows:
        ended = normalize_ts(
            datetime.fromisoformat(r["ended_at"].replace("Z", "+00:00"))
        )
        if window_start <= ended < window_end:
            trips.append(
                {
                    "trip_id": r["trip_id"],
                    "passenger_id": r["passenger_id"],
                    "from_station": r["from_station"],
                    "to_station": r["to_station"],
                    "ended": ended,
                    "ended_at": iso_str(ended),
                    "amount_cents": r["amount_cents"],
                }
            )
    trips.sort(key=lambda t: (t["ended"], t["trip_id"]))
    return trips


def prior_operator_totals(
    conn, root_clearing_id: str, before_seq: int
) -> dict[tuple[str, str], int]:
    """Cumulative cents already booked per (trip_id, operator_id) by every
    batch of the chain with seq < before_seq (root = seq 0)."""
    rows = conn.execute(
        "SELECT e.trip_id, e.operator_id, SUM(e.amount_cents) AS cumulative"
        " FROM clearing_entries e"
        " JOIN clearing_batches b ON b.clearing_id = e.clearing_id"
        " WHERE b.root_clearing_id = ? AND b.seq < ?"
        " GROUP BY e.trip_id, e.operator_id",
        (root_clearing_id, before_seq),
    ).fetchall()
    return {(r["trip_id"], r["operator_id"]): r["cumulative"] for r in rows}


def last_seen_trips(
    conn, root_clearing_id: str, before_seq: int
) -> dict[str, dict]:
    """Last trip descriptor (passenger, stations, tap-out time, gross) recorded
    by earlier batches, keyed by trip_id. Rows stream in (batch_seq, entry seq)
    order, so simply overwriting keeps the newest batch's view."""
    rows = conn.execute(
        "SELECT e.trip_id, e.passenger_id, e.from_station, e.to_station,"
        " e.ended_at, e.gross_amount_cents"
        " FROM clearing_entries e"
        " JOIN clearing_batches b ON b.clearing_id = e.clearing_id"
        " WHERE b.root_clearing_id = ? AND b.seq < ?"
        " ORDER BY b.seq, e.seq",
        (root_clearing_id, before_seq),
    ).fetchall()
    latest: dict[str, dict] = {}
    for r in rows:
        latest[r["trip_id"]] = {
            "trip_id": r["trip_id"],
            "passenger_id": r["passenger_id"],
            "from_station": r["from_station"],
            "to_station": r["to_station"],
            "ended_at": r["ended_at"],
            "gross_amount_cents": r["gross_amount_cents"],
        }
    return latest


def plan_adjustment_entries(
    rules_by_pair: dict[tuple[str, str], list[tuple[str, int]]],
    current_trips: list[dict],
    prior: dict[tuple[str, str], int],
    seen_before: dict[str, dict],
    trip_filter: set[str] | None = None,
) -> dict:
    """Compute the adjustment batch that would bring one clearing chain from
    its booked positions to the targets implied by ``current_trips``.

    Pure: no database access, no persistence. For every current trip the
    per-operator target is split from the current charge and reduced by the
    cumulative amount already booked across earlier chain batches; trips the
    chain has seen but that are no longer present are reversed to zero.

    ``trip_filter`` restricts the output to a set of trip ids (used by the
    correction preview, which only reports chain effects attributable to the
    simulated correction); chain-level problems such as frozen-rule coverage
    gaps are still computed for the whole current window.

    Returns ``entries`` (signed per-trip / per-operator delta dicts, sorted by
    ended_at/trip_id/operator), ``gross_total`` (sum of the current trips'
    grosses) and ``missing_pairs`` (directed station pairs of current trips the
    frozen rules do not cover).
    """
    missing = sorted(
        {f"{t['from_station']}->{t['to_station']}"
         for t in current_trips
         if (t["from_station"], t["to_station"]) not in rules_by_pair}
    )

    current_ids = {t["trip_id"] for t in current_trips}
    vanished_ids = sorted(set(seen_before) - current_ids)
    if trip_filter is not None:
        vanished_ids = [tid for tid in vanished_ids if tid in trip_filter]

    entries: list[dict] = []
    gross_total = 0
    reversed_count = 0
    prior_ops_by_trip: dict[str, set[str]] = {}
    for (tid, op) in prior:
        prior_ops_by_trip.setdefault(tid, set()).add(op)
    for trip in current_trips:
        pair = (trip["from_station"], trip["to_station"])
        if pair not in rules_by_pair:
            # Coverage gap: reported separately via missing_pairs. The caller
            # decides whether to reject (the live endpoint, 422) or report it
            # (the correction preview), so no target split is attempted here.
            continue
        shares = rules_by_pair[pair]
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
    if trip_filter is not None:
        entries = [e for e in entries if e["trip_id"] in trip_filter]
    entries.sort(key=lambda e: (e["ended_at"], e["trip_id"], e["operator_id"]))
    return {
        "entries": entries,
        "gross_total": gross_total,
        "missing_pairs": missing,
        "reversed_count": reversed_count,
        "current_trip_count": len(current_trips),
    }
