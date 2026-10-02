"""External deduction reconciliation.

A reconciliation batch snapshots the internal trip charges whose tap-out
time (``ended_at``) falls inside the half-open UTC window
``[window_start, window_end)`` and matches them exactly once against the
submitted external deduction records:

1. external records whose ``trip_id`` exactly identifies a snapshot trip of
   the same passenger are locked first;
2. the remaining records are paired per passenger through a one-to-one
   matching that first maximises the number of pairs and then minimises the
   total absolute time difference. When several equally optimal pairings
   exist, every record whose partner is not identical across all of them is
   reported as ``ambiguous`` instead of being paired by submission order.

Results are computed once, at batch creation, and persisted, so later event
corrections never change an already produced batch.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta

from .settlement import iso_str

CATEGORIES = (
    "matched",
    "overcharged",
    "undercharged",
    "internal_only",
    "external_only",
    "ambiguous",
)


@dataclass(frozen=True)
class SnapshotTrip:
    trip_id: str
    passenger_id: str
    ended_at: datetime  # tz-aware UTC
    amount_cents: int


@dataclass(frozen=True)
class ExternalRecord:
    serial_no: str
    passenger_id: str
    charged_at: datetime  # tz-aware UTC
    amount_cents: int
    trip_id: str | None


def _max_pairs_min_cost(
    n_left: int, n_right: int, edges: list[tuple[int, int, int]]
) -> tuple[int, int]:
    """Return ``(pairs, total_cost)`` for the bipartite matching that first
    maximises the pair count and then minimises the total edge cost.

    Successive shortest augmenting paths on the unit-capacity flow network
    source -> left -> right -> sink; SPFA relaxations cope with the negative
    residual edges. Per-passenger inputs are small, so clarity wins over
    asymptotics.
    """
    if not edges or n_left == 0 or n_right == 0:
        return 0, 0
    n = n_left + n_right + 2
    src, snk = n - 2, n - 1
    adj: list[list[list[int]]] = [[] for _ in range(n)]

    def add(u: int, v: int, cap: int, cost: int) -> None:
        adj[u].append([v, cap, cost, len(adj[v])])
        adj[v].append([u, 0, -cost, len(adj[u]) - 1])

    for i in range(n_left):
        add(src, i, 1, 0)
    for j in range(n_right):
        add(n_left + j, snk, 1, 0)
    for i, j, w in edges:
        add(i, n_left + j, 1, w)

    pairs = 0
    total = 0
    while True:
        dist: list[int | None] = [None] * n
        prev: list[tuple[int, int] | None] = [None] * n
        in_queue = [False] * n
        dist[src] = 0
        queue = deque([src])
        in_queue[src] = True
        while queue:
            u = queue.popleft()
            in_queue[u] = False
            du = dist[u]
            for ei, (v, cap, cost, _rev) in enumerate(adj[u]):
                if cap > 0 and (dist[v] is None or du + cost < dist[v]):
                    dist[v] = du + cost
                    prev[v] = (u, ei)
                    if not in_queue[v]:
                        queue.append(v)
                        in_queue[v] = True
        if prev[snk] is None:
            return pairs, total
        v = snk
        while v != src:
            u, ei = prev[v]
            edge = adj[u][ei]
            edge[1] -= 1
            adj[v][edge[3]][1] += 1
            total += edge[2]
            v = u
        pairs += 1


def _optimal_without(
    n_left: int,
    n_right: int,
    edges: list[tuple[int, int, int]],
    drop_left: tuple[int, ...] = (),
    drop_right: tuple[int, ...] = (),
) -> tuple[int, int]:
    """Optimal ``(pairs, total_cost)`` with the given nodes removed."""
    keep_l = [i for i in range(n_left) if i not in drop_left]
    keep_r = [j for j in range(n_right) if j not in drop_right]
    li = {old: new for new, old in enumerate(keep_l)}
    ri = {old: new for new, old in enumerate(keep_r)}
    sub = [(li[i], ri[j], w) for i, j, w in edges if i in li and j in ri]
    return _max_pairs_min_cost(len(keep_l), len(keep_r), sub)


def _fmt_us(us: int) -> str:
    """Format a microsecond duration as a compact seconds string."""
    if us % 1_000_000 == 0:
        return f"{us // 1_000_000}s"
    return f"{us / 1_000_000:.6f}".rstrip("0").rstrip(".") + "s"


def _category_for(diff: int) -> str:
    if diff == 0:
        return "matched"
    return "overcharged" if diff > 0 else "undercharged"


def _amount_phrase(internal: int, external: int) -> str:
    diff = external - internal
    if diff == 0:
        return "amounts equal"
    if diff > 0:
        return f"external overcharged by {diff} cents"
    return f"external undercharged by {-diff} cents"


def _pair_row(
    trip: SnapshotTrip,
    rec: ExternalRecord,
    method: str,
    dt_us: int | None,
    tolerance_seconds: int,
    note: str | None,
) -> dict:
    diff = rec.amount_cents - trip.amount_cents
    if method == "trip_id":
        how = "locked by exact trip_id"
    else:
        how = (
            f"paired by time tolerance "
            f"(|time diff| {_fmt_us(dt_us)} <= tolerance {tolerance_seconds}s)"
        )
    reason = f"{how}; {_amount_phrase(trip.amount_cents, rec.amount_cents)}"
    if note:
        reason += f"; {note}"
    return {
        "category": _category_for(diff),
        "passenger_id": trip.passenger_id,
        "trip_id": trip.trip_id,
        "serial_no": rec.serial_no,
        "internal_amount_cents": trip.amount_cents,
        "external_amount_cents": rec.amount_cents,
        "difference_cents": diff,
        "internal_ended_at": iso_str(trip.ended_at),
        "external_charged_at": iso_str(rec.charged_at),
        "match_method": method,
        "reason": reason,
        "_ts": trip.ended_at,
    }


def _trip_row(trip: SnapshotTrip, category: str, reason: str) -> dict:
    return {
        "category": category,
        "passenger_id": trip.passenger_id,
        "trip_id": trip.trip_id,
        "serial_no": None,
        "internal_amount_cents": trip.amount_cents,
        "external_amount_cents": None,
        "difference_cents": None,
        "internal_ended_at": iso_str(trip.ended_at),
        "external_charged_at": None,
        "match_method": None,
        "reason": reason,
        "_ts": trip.ended_at,
    }


def _record_row(rec: ExternalRecord, category: str, reason: str) -> dict:
    return {
        "category": category,
        "passenger_id": rec.passenger_id,
        "trip_id": None,
        "serial_no": rec.serial_no,
        "internal_amount_cents": None,
        "external_amount_cents": rec.amount_cents,
        "difference_cents": None,
        "internal_ended_at": None,
        "external_charged_at": iso_str(rec.charged_at),
        "match_method": None,
        "reason": reason,
        "_ts": rec.charged_at,
    }


def _match_passenger(
    trips: list[SnapshotTrip],
    recs: list[ExternalRecord],
    tolerance_us: int,
    tolerance_seconds: int,
    notes: dict[str, str],
) -> list[dict]:
    """One-to-one matching for a single passenger. A pair is reported only
    when it is part of *every* optimal matching; anything whose pairing
    varies across equally optimal solutions is listed as ambiguous."""
    n_left, n_right = len(trips), len(recs)
    edges: list[tuple[int, int, int]] = []
    for i, t in enumerate(trips):
        for j, r in enumerate(recs):
            us = abs(t.ended_at - r.charged_at) // timedelta(microseconds=1)
            if us <= tolerance_us:
                edges.append((i, j, us))

    pairs, cost = _max_pairs_min_cost(n_left, n_right, edges)
    optimum = f"pairs={pairs}, total |time diff|={_fmt_us(cost)}"
    edge_cost = {(i, j): w for i, j, w in edges}
    left_has_edge = {i for i, _, _ in edges}
    right_has_edge = {j for _, j, _ in edges}

    # Partner possibilities: edge (i, j) is feasible in some optimal matching
    # iff forcing it still reaches the global optimum.
    trip_partners: list[list[int]] = [[] for _ in range(n_left)]
    rec_partners: list[list[int]] = [[] for _ in range(n_right)]
    for i, j, w in edges:
        sub = _optimal_without(n_left, n_right, edges, (i,), (j,))
        if sub == (pairs - 1, cost - w):
            trip_partners[i].append(j)
            rec_partners[j].append(i)

    # A node is "free" iff some optimal matching leaves it unmatched.
    trip_free = [
        _optimal_without(n_left, n_right, edges, (i,)) == (pairs, cost)
        for i in range(n_left)
    ]
    rec_free = [
        _optimal_without(n_left, n_right, edges, (), (j,)) == (pairs, cost)
        for j in range(n_right)
    ]

    rows: list[dict] = []
    paired_recs: set[int] = set()
    for i, trip in enumerate(trips):
        partners = trip_partners[i]
        if not partners:
            if i in left_has_edge:
                reason = f"unmatched in every optimal pairing ({optimum})"
            else:
                reason = (
                    f"no external record for this passenger within "
                    f"tolerance {tolerance_seconds}s"
                )
            rows.append(_trip_row(trip, "internal_only", reason))
        elif trip_free[i] or len(partners) > 1:
            cand = ", ".join(
                f"'{recs[j].serial_no}' (|time diff| {_fmt_us(edge_cost[(i, j)])})"
                for j in partners
            )
            reason = (
                f"ambiguous: {len(partners)} possible partner(s) across equally "
                f"optimal pairings ({optimum}); candidate serial_no(s): {cand}"
            )
            if trip_free[i]:
                reason += "; may also be left unmatched"
            rows.append(_trip_row(trip, "ambiguous", reason))
        else:
            j = partners[0]
            paired_recs.add(j)
            rows.append(
                _pair_row(
                    trip, recs[j], "tolerance", edge_cost[(i, j)],
                    tolerance_seconds, notes.get(recs[j].serial_no),
                )
            )

    for j, rec in enumerate(recs):
        if j in paired_recs:
            continue
        partners = rec_partners[j]
        note = notes.get(rec.serial_no)
        if not partners:
            if j in right_has_edge:
                reason = f"unmatched in every optimal pairing ({optimum})"
            else:
                reason = (
                    f"no internal trip for this passenger within "
                    f"tolerance {tolerance_seconds}s"
                )
            if note:
                reason += f"; {note}"
            rows.append(_record_row(rec, "external_only", reason))
        elif rec_free[j] or len(partners) > 1:
            cand = ", ".join(
                f"'{trips[i].trip_id}' (|time diff| {_fmt_us(edge_cost[(i, j)])})"
                for i in partners
            )
            reason = (
                f"ambiguous: {len(partners)} possible partner(s) across equally "
                f"optimal pairings ({optimum}); candidate trip_id(s): {cand}"
            )
            if rec_free[j]:
                reason += "; may also be left unmatched"
            if note:
                reason += f"; {note}"
            rows.append(_record_row(rec, "ambiguous", reason))
        # else: uniquely paired — already emitted from the trip side.
    return rows


def reconcile(
    snapshot: list[SnapshotTrip],
    records: list[ExternalRecord],
    tolerance_seconds: int,
) -> list[dict]:
    """Match one snapshot against one set of external records and return the
    result rows (modelled on ``ReconResultItem``), deterministically ordered
    by passenger, time and identifiers — never by submission order."""
    tolerance_us = tolerance_seconds * 1_000_000
    rows: list[dict] = []

    trips_by_id = {t.trip_id: t for t in snapshot}
    consumed_trips: set[str] = set()
    consumed_serials: set[str] = set()
    notes: dict[str, str] = {}  # serial_no -> why its trip_id was not used

    # Phase 1: exact trip_id locks.
    claims: dict[str, list[ExternalRecord]] = {}
    for rec in records:
        if rec.trip_id:
            claims.setdefault(rec.trip_id, []).append(rec)

    for trip_id in sorted(claims):
        group = sorted(claims[trip_id], key=lambda r: r.serial_no)
        trip = trips_by_id.get(trip_id)
        if trip is None:
            for rec in group:
                notes[rec.serial_no] = (
                    f"provided trip_id '{trip_id}' not found in window snapshot"
                )
            continue
        eligible = [r for r in group if r.passenger_id == trip.passenger_id]
        for rec in group:
            if rec.passenger_id != trip.passenger_id:
                notes[rec.serial_no] = (
                    f"provided trip_id '{trip_id}' belongs to passenger "
                    f"'{trip.passenger_id}'"
                )
        if len(eligible) == 1:
            rec = eligible[0]
            consumed_trips.add(trip_id)
            consumed_serials.add(rec.serial_no)
            rows.append(_pair_row(trip, rec, "trip_id", None, tolerance_seconds, None))
        elif len(eligible) > 1:
            serials = ", ".join(f"'{r.serial_no}'" for r in eligible)
            consumed_trips.add(trip_id)
            rows.append(
                _trip_row(
                    trip, "ambiguous",
                    f"ambiguous: trip_id claimed by multiple external records: {serials}",
                )
            )
            for rec in eligible:
                consumed_serials.add(rec.serial_no)
                rows.append(
                    _record_row(
                        rec, "ambiguous",
                        f"ambiguous: multiple external records claim trip_id "
                        f"'{trip_id}': {serials}",
                    )
                )

    # Phase 2: per-passenger tolerance matching on what remains.
    remaining_trips: dict[str, list[SnapshotTrip]] = {}
    for t in snapshot:
        if t.trip_id not in consumed_trips:
            remaining_trips.setdefault(t.passenger_id, []).append(t)
    remaining_recs: dict[str, list[ExternalRecord]] = {}
    for r in records:
        if r.serial_no not in consumed_serials:
            remaining_recs.setdefault(r.passenger_id, []).append(r)

    for pid in sorted(set(remaining_trips) | set(remaining_recs)):
        trips = sorted(
            remaining_trips.get(pid, []), key=lambda t: (t.ended_at, t.trip_id)
        )
        recs = sorted(remaining_recs.get(pid, []), key=lambda r: r.serial_no)
        rows.extend(_match_passenger(trips, recs, tolerance_us, tolerance_seconds, notes))

    rows.sort(
        key=lambda r: (r["passenger_id"], r["_ts"], r["trip_id"] or "", r["serial_no"] or "")
    )
    for r in rows:
        del r["_ts"]
    return rows
