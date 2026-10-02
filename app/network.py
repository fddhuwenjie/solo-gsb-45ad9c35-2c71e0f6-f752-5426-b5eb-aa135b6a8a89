"""Transfer network validation.

A validation request declares a half-open UTC effective window, the served
stations, designated hubs, directed transit line segments and walking
transfer edges; every edge carries its own half-open active interval. The
timeline is sliced at the window bounds and at every (clipped) edge bound,
so the directed graph is static inside each slice.

The checks run in a fixed priority order and the first violation wins,
which makes the reported conflict deterministic:

1. ``self_loop_edge``       -- an edge whose endpoints are the same station
   (reported at the earliest slice in which it is active);
2. ``overlapping_interval`` -- the same declared edge identity (line +
   directed pair for segments, directed pair for walking transfers) has two
   active windows overlapping with positive duration;
3. ``duplicate_edge``       -- two distinct edge identities offer the same
   directed pair in the same slice (parallel graph edges);
4. ``transfer_asymmetry``   -- a walking transfer A->B is active in a slice
   while no reverse walking transfer B->A is; the shortest detour B->A (if
   any exists) is attached as evidence;
5. ``unreachable_station``  -- some hub cannot reach some served station in
   the slice (shortest evidence path does not exist, hence is null).

Checks 3-5 are per-slice and slices are examined chronologically. All
intervals are half-open ``[start, end)``: touching windows are legal, and
edge intervals are clipped to the effective window (edges with no
intersection never participate).
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime

from .settlement import iso_str, normalize_ts


@dataclass(frozen=True)
class NetEdge:
    kind: str  # "segment" | "transfer"
    from_station: str
    to_station: str
    start: datetime  # tz-aware UTC, as declared (normalised)
    end: datetime
    line_id: str | None = None
    segment_id: str | None = None


def _edge_ref(e: NetEdge) -> dict:
    ref = {
        "edge_kind": e.kind,
        "from_station": e.from_station,
        "to_station": e.to_station,
        "start_at": iso_str(e.start),
        "end_at": iso_str(e.end),
    }
    if e.kind == "segment":
        ref["line_id"] = e.line_id
        ref["segment_id"] = e.segment_id
    return ref


def _hop(e: NetEdge) -> dict:
    hop = {
        "from_station": e.from_station,
        "to_station": e.to_station,
        "edge_kind": e.kind,
    }
    if e.kind == "segment":
        hop["line_id"] = e.line_id
        hop["segment_id"] = e.segment_id
    return hop


def _edge_sort(e: NetEdge) -> tuple:
    return (
        e.kind, e.from_station, e.to_station,
        e.line_id or "", e.segment_id or "", iso_str(e.start),
    )


def _identity(e: NetEdge) -> tuple:
    """The declared edge identity: a line section, or a walking corridor."""
    if e.kind == "segment":
        return ("segment", e.line_id or "", e.from_station, e.to_station)
    return ("transfer", e.from_station, e.to_station)


def _build_slices(
    win_start: datetime,
    win_end: datetime,
    edges: list[NetEdge],
) -> list[tuple[int, datetime, datetime, list[NetEdge]]]:
    """Slice [win_start, win_end) at every clipped edge bound and attach the
    active edges to each half-open slice."""
    bounds = {win_start, win_end}
    kept: list[tuple[NetEdge, datetime, datetime]] = []
    for e in edges:
        cs = max(e.start, win_start)
        ce = min(e.end, win_end)
        if cs < ce:  # needs a non-empty intersection
            kept.append((e, cs, ce))
            bounds.add(cs)
            bounds.add(ce)

    times = sorted(bounds)
    slices: list[tuple[int, datetime, datetime, list[NetEdge]]] = []
    for i, (s, t) in enumerate(zip(times, times[1:])):
        active = [e for e, cs, ce in kept if cs <= s and ce >= t]
        active.sort(key=_edge_sort)
        slices.append((i, s, t, active))
    return slices


def _adjacency(active: list[NetEdge]) -> dict[str, list[NetEdge]]:
    adj: dict[str, list[NetEdge]] = {}
    for e in active:
        adj.setdefault(e.from_station, []).append(e)
    for outs in adj.values():
        outs.sort(key=lambda e: (e.to_station,) + _edge_sort(e))
    return adj


def _shortest_path(
    adj: dict[str, list[NetEdge]],
    src: str,
    dst: str,
) -> list[dict] | None:
    """BFS shortest path (fewest hops); ties break on the lexicographically
    smallest station sequence. Returns hop descriptors, [] for src == dst,
    or None when unreachable."""
    if src == dst:
        return []

    dist: dict[str, int] = {src: 0}
    queue = deque([src])
    while queue:
        u = queue.popleft()
        for e in adj.get(u, []):
            if e.to_station not in dist:
                dist[e.to_station] = dist[u] + 1
                queue.append(e.to_station)
    if dst not in dist:
        return None

    # Reverse BFS from dst: nodes on a shortest src->dst path satisfy
    # dist[u] + rdist[u] == dist[dst].
    radj: dict[str, list[NetEdge]] = {}
    for u, outs in adj.items():
        for e in outs:
            radj.setdefault(e.to_station, []).append(e)
    rdist = {dst: 0}
    queue = deque([dst])
    while queue:
        u = queue.popleft()
        for e in radj.get(u, []):
            if e.from_station not in rdist and e.from_station in dist:
                rdist[e.from_station] = rdist[u] + 1
                queue.append(e.from_station)

    total = dist[dst]
    hops: list[dict] = []
    cur = src
    while cur != dst:
        candidates = [
            e for e in adj.get(cur, [])
            if e.to_station in dist
            and dist[e.to_station] == dist[cur] + 1
            and dist[e.to_station] + rdist.get(e.to_station, total + 1) == total
        ]
        chosen = sorted(candidates, key=lambda e: (e.to_station,) + _edge_sort(e))[0]
        hops.append(_hop(chosen))
        cur = chosen.to_station
    return hops


def _slice_ref(index: int, start: datetime, end: datetime) -> dict:
    return {"index": index, "start": iso_str(start), "end": iso_str(end)}


def validate_network(
    win_start: datetime,
    win_end: datetime,
    stations: list[str],
    hubs: list[str],
    edges: list[NetEdge],
) -> dict:
    """Run every check and return the slice list and the first conflict."""
    win_start = normalize_ts(win_start)
    win_end = normalize_ts(win_end)

    # Clip edge windows up front; edges disjoint from the effective window
    # never participate.
    clipped: list[tuple[NetEdge, datetime, datetime]] = []
    for e in edges:
        cs = max(e.start, win_start)
        ce = min(e.end, win_end)
        if cs < ce:
            clipped.append((e, cs, ce))

    bounds = sorted(
        {win_start, win_end}
        | {cs for _, cs, _ in clipped}
        | {ce for _, _, ce in clipped}
    )

    def empty_report() -> dict:
        return {
            "slice_count": len(bounds) - 1,
            "slices": [
                _slice_ref(i, s, t)
                for i, (s, t) in enumerate(zip(bounds, bounds[1:]))
            ],
        }

    def slice_at(ts: datetime) -> dict:
        index = bounds.index(ts)
        return _slice_ref(index, bounds[index], bounds[index + 1])

    def finished(conflict: dict) -> dict:
        report = empty_report()
        report["conflict"] = conflict
        return report

    # 1. Self loops: earliest active slice of the offending edge.
    for e, cs, _ce in sorted(clipped, key=lambda x: (x[1], _edge_sort(x[0]))):
        if e.from_station == e.to_station:
            kind = "line segment" if e.kind == "segment" else "walking transfer"
            return finished({
                "code": "self_loop_edge",
                "slice": slice_at(cs),
                "message": (
                    f"self-loop {kind} {e.from_station} -> {e.to_station}"
                    " is not allowed"
                ),
                "edge": _edge_ref(e),
                "evidence_path": None,
            })

    # 2. Overlapping effective intervals of the same declared edge identity.
    by_identity: dict[tuple, list[tuple[NetEdge, datetime, datetime]]] = {}
    for item in clipped:
        by_identity.setdefault(_identity(item[0]), []).append(item)
    for key in sorted(by_identity):
        grp = sorted(by_identity[key], key=lambda x: (x[1], _edge_sort(x[0])))
        for (e1, _s1, t1), (e2, s2, _t2) in zip(grp, grp[1:]):
            if s2 < t1:  # half-open: touching windows (s2 == t1) are legal
                if e1.kind == "segment":
                    message = (
                        f"line '{e1.line_id}' segment {e1.from_station} ->"
                        f" {e1.to_station} has overlapping effective intervals:"
                        f" '{e1.segment_id}' and '{e2.segment_id}'"
                    )
                else:
                    message = (
                        f"walking transfer {e1.from_station} ->"
                        f" {e1.to_station} is declared with overlapping"
                        " effective intervals"
                    )
                return finished({
                    "code": "overlapping_interval",
                    "slice": slice_at(s2),
                    "message": message,
                    "edges": [_edge_ref(e1), _edge_ref(e2)],
                    "evidence_path": None,
                })

    # Per-slice checks, slices examined chronologically.
    slices = _build_slices(win_start, win_end, edges)
    for index, s, t, active in slices:
        slc = _slice_ref(index, s, t)

        # 3. Parallel edges: distinct identities on the same directed pair.
        by_pair: dict[tuple, list[NetEdge]] = {}
        for e in active:
            by_pair.setdefault((e.from_station, e.to_station), []).append(e)
        for pair in sorted(by_pair):
            grp = by_pair[pair]
            distinct: dict[tuple, NetEdge] = {}
            for e in grp:
                distinct.setdefault(_identity(e), e)
            if len(distinct) > 1:
                picked = [distinct[k] for k in sorted(distinct)[:2]]
                a, b = pair
                return finished({
                    "code": "duplicate_edge",
                    "slice": slc,
                    "message": (
                        f"duplicate directed edge {a} -> {b}: two distinct"
                        " service declarations are active in the same slice"
                    ),
                    "edges": [_edge_ref(e) for e in picked],
                    "evidence_path": None,
                })

        adj = _adjacency(active)

        # 4. Walking transfers must come in symmetric pairs within the slice.
        transfer_pairs: dict[tuple, NetEdge] = {}
        for e in active:
            if e.kind == "transfer":
                transfer_pairs[(e.from_station, e.to_station)] = e
        for a, b in sorted(transfer_pairs):
            if (b, a) not in transfer_pairs:
                e = transfer_pairs[(a, b)]
                evidence = _shortest_path(adj, b, a)
                if evidence:
                    tail = (
                        "; shortest alternative return path"
                        f" {b} -> {a} takes {len(evidence)} hop(s)"
                    )
                else:
                    tail = f"; no return path from {b} back to {a} exists"
                return finished({
                    "code": "transfer_asymmetry",
                    "slice": slc,
                    "message": (
                        f"walking transfer {a} -> {b} has no reverse walking"
                        f" transfer {b} -> {a} active in this slice{tail}"
                    ),
                    "edge": _edge_ref(e),
                    "evidence_path": evidence,
                })

        # 5. Every hub must reach every served station inside the slice.
        for hub in sorted(hubs):
            reachable = {hub}
            queue = deque([hub])
            while queue:
                u = queue.popleft()
                for e in adj.get(u, []):
                    if e.to_station not in reachable:
                        reachable.add(e.to_station)
                        queue.append(e.to_station)
            for station in sorted(stations):
                if station not in reachable:
                    return finished({
                        "code": "unreachable_station",
                        "slice": slc,
                        "message": (
                            f"station {station} is not reachable from hub"
                            f" {hub} in this slice"
                        ),
                        "hub": hub,
                        "station": station,
                        "evidence_path": None,
                    })

    report = empty_report()
    report["conflict"] = None
    return report
