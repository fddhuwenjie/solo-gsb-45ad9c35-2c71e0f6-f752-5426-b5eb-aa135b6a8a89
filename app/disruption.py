"""Sudden-disruption evacuation replanning.

Given a timetable of replacement services (each a sequence of timed stops with
one rated capacity shared by every section of the service) and a set of
indivisible passenger groups, assign each group the itinerary that gets the
whole party to its destination earliest, tie-breaking first on fewer transfers
and then on the lexicographically smaller sequence of service ids. Groups are
planned in ``(ready_at, group_id)`` order; an assignment decrements the
remaining capacity of every section of every service used, so later groups see
the seats already consumed. A group that cannot be carried at all is reported
with the first obstruction it meets (unserved endpoint, unreachable
destination, or insufficient section capacity).

The search is a multi-label shortest path over stations. A label at a station
is ``(arrival_at, rides, service_ids)`` plus the stations/services it has
used; riding a service that calls at the current station extends a label to
any later stop on that service, provided the party is ready in time (the
``min_transfer_seconds`` gap is enforced between different services, never
for the first boarding) and every traversed section still has enough
remaining capacity. Services are never reused within one itinerary:
revisiting a service cannot beat simply staying on it.

Each station keeps every settled label that is still non-dominated (see
``_dominates``) instead of a single best one: when two itineraries converge
at a transfer station, the one that arrives earlier but has already used
more rides does not necessarily win -- the later one may board the same
final service, arrive at the destination at the same moment and then take
the fewer-transfers tie-break. Discarding it early would lose exactly those
optima.

Results are computed once and frozen by the caller: this module is a pure
function of its inputs and mutates only the in-memory capacity ledger.
"""
from __future__ import annotations

import hashlib
import heapq
from dataclasses import dataclass, field
from datetime import datetime

from .audit import canonical_json
from .settlement import iso_str, normalize_ts

ORIGIN_NOT_SERVED = "origin_not_served"
DESTINATION_NOT_SERVED = "destination_not_served"
UNREACHABLE_DESTINATION = "unreachable_destination"
INSUFFICIENT_CAPACITY = "insufficient_capacity"

_ORIGIN_MESSAGE = "no replacement service calls at the origin station"
_DESTINATION_MESSAGE = "no replacement service calls at the destination station"
_UNREACHABLE_MESSAGE = "no feasible itinerary reaches the destination from the origin"
_CAPACITY_MESSAGE = (
    "no itinerary has enough remaining section capacity for the whole party"
)


@dataclass(frozen=True)
class Stop:
    station_id: str
    arrival_at: datetime
    departure_at: datetime


@dataclass(frozen=True)
class Service:
    service_id: str
    capacity: int
    stops: tuple[Stop, ...]


@dataclass(frozen=True)
class Group:
    group_id: str
    origin: str
    destination: str
    ready_at: datetime
    party_size: int


@dataclass
class _Assignment:
    legs: tuple[dict, ...]
    service_ids: tuple[str, ...]
    arrival_at: datetime
    transfers: int


def _dominates(a: tuple, b: tuple) -> bool:
    """True when label ``a`` makes same-station label ``b`` unable to win.

    Labels are ``(arrival_at, rides, service_ids, used_services, visited)``.
    ``a`` must arrive no later and use no more rides; when the ride counts
    tie, its service-id sequence must be lexicographically no greater (the
    final tie-break; with fewer rides ``a`` already wins on transfers, so the
    sequences are only compared at equal length). Additionally every
    continuation still open to ``b`` -- services not yet used and stations
    not yet visited -- must also be open to ``a``; otherwise ``b`` might
    reach the destination along a route ``a`` can no longer take. Capacity
    needs no say here: the remaining section capacity of any continuation is the
    same for both labels, so a completion feasible for ``b`` is feasible for
    ``a`` as well, and scores no better.
    """
    a_time, a_rides, a_ids, a_used, a_visited = a
    b_time, b_rides, b_ids, b_used, b_visited = b
    if a_time > b_time or a_rides > b_rides:
        return False
    if a_rides == b_rides and a_ids > b_ids:
        return False
    return a_used <= b_used and a_visited <= b_visited


@dataclass
class _Planner:
    services: list[Service]
    min_transfer_seconds: int
    # Remaining seats keyed (service_id, section_index); section i runs from
    # stop i to stop i+1 of the service.
    remaining: dict[tuple[str, int], int]
    # service_id -> {station_id: stop_index}
    stop_index: dict[str, dict[str, int]] = field(default_factory=dict)
    # station_id -> services (in input service order) that call there
    calling: dict[str, list[Service]] = field(default_factory=dict)

    def __post_init__(self):
        for svc in self.services:
            self.stop_index[svc.service_id] = {
                stop.station_id: i for i, stop in enumerate(svc.stops)
            }
            for stop in svc.stops:
                self.calling.setdefault(stop.station_id, []).append(svc)

    def _fits(self, svc: Service, board: int, alight: int, party_size: int) -> bool:
        return all(
            self.remaining[(svc.service_id, i)] >= party_size
            for i in range(board, alight)
        )

    def search(
        self,
        group: Group,
        *,
        enforce_capacity: bool,
    ) -> _Assignment | None:
        """Earliest-arrival itinerary under the label ordering
        ``(arrival_at, rides, service_ids)``. Returns None when no itinerary
        reaches the destination under the current constraints.

        Every station keeps the Pareto front of its settled labels: a label
        is discarded only when a settled same-station label dominates it
        (``_dominates``), i.e. the kept label is at least as good on every
        component of the objective and can still take every continuation the
        discarded one could. This is what lets a later-arriving but
        fewer-rides itinerary survive at a transfer station when both can
        board the same final service and arrive together.

        The heap key is exactly the objective tuple, so the first label
        popped at the destination is the optimum: every extension only moves
        arrival later, adds a ride and appends to the service sequence, and
        any label pruned along the way is matched by a surviving label whose
        best completion is at least as good. Labels that revisit a station
        or a service are never generated -- an itinerary with such a loop is
        strictly worse (later arrival, more rides) than the same itinerary
        with the loop removed, so it can never be optimal.
        """
        # Empty-leg journeys (origin == destination) are always trivially
        # feasible and consume no capacity.
        if group.origin == group.destination:
            return _Assignment((), (), group.ready_at, 0)

        # Heap labels: (arrival_at, rides, service_ids, station, visited, legs)
        heap: list = [
            (group.ready_at, 0, (), group.origin, (group.origin,), ())
        ]
        # Non-dominated settled labels per station, each stored as
        # (arrival_at, rides, service_ids, used_services, visited_stations).
        settled: dict[str, list[tuple]] = {}

        while heap:
            arrived_at, rides, service_ids, station, visited, legs = heapq.heappop(heap)
            if station == group.destination:
                return _Assignment(
                    legs, service_ids, arrived_at, rides - 1 if rides else 0
                )
            current = (
                arrived_at,
                rides,
                service_ids,
                frozenset(service_ids),
                frozenset(visited),
            )
            labels = settled.get(station, [])
            if any(_dominates(kept, current) for kept in labels):
                continue
            # Settle the label: it joins the front and evicts any settled
            # label it now dominates.
            labels = [kept for kept in labels if not _dominates(current, kept)]
            labels.append(current)
            settled[station] = labels

            for svc in self.calling.get(station, []):
                if svc.service_id in service_ids:
                    continue
                board = self.stop_index[svc.service_id][station]
                board_dep = svc.stops[board].departure_at
                # First boarding only needs to follow ready_at; every later
                # boarding must honour the minimum transfer gap.
                if rides == 0:
                    if board_dep < group.ready_at:
                        continue
                elif (board_dep - arrived_at).total_seconds() < self.min_transfer_seconds:
                    continue
                for alight in range(board + 1, len(svc.stops)):
                    if enforce_capacity and not self._fits(svc, board, alight, group.party_size):
                        continue
                    next_station = svc.stops[alight].station_id
                    if next_station in visited:
                        continue
                    next_arrival = svc.stops[alight].arrival_at
                    next_ids = service_ids + (svc.service_id,)
                    # A candidate already dominated by a settled label at its
                    # station can never win; don't push it.
                    candidate = (
                        next_arrival,
                        rides + 1,
                        next_ids,
                        frozenset(next_ids),
                        frozenset(visited + (next_station,)),
                    )
                    if any(
                        _dominates(kept, candidate)
                        for kept in settled.get(next_station, [])
                    ):
                        continue
                    leg = {
                        "service_id": svc.service_id,
                        "from_station": station,
                        "to_station": next_station,
                        "departure_at": iso_str(board_dep),
                        "arrival_at": iso_str(next_arrival),
                    }
                    heapq.heappush(
                        heap,
                        (
                            next_arrival,
                            rides + 1,
                            next_ids,
                            next_station,
                            visited + (next_station,),
                            legs + (leg,),
                        ),
                    )
        return None

    def consume(self, assignment: _Assignment, party_size: int) -> None:
        for leg in assignment.legs:
            svc_id = leg["service_id"]
            board = self.stop_index[svc_id][leg["from_station"]]
            alight = self.stop_index[svc_id][leg["to_station"]]
            for i in range(board, alight):
                self.remaining[(svc_id, i)] -= party_size


def build_replan(
    min_transfer_seconds: int,
    raw_services: list,
    raw_groups: list,
) -> dict:
    """Plan every group in deterministic processing order and return the frozen
    result body (without the replay marker). Each group is planned against the
    capacity ledger left by earlier assignments.

    ``raw_services`` / ``raw_groups`` are the already normalised dataclasses
    produced by :func:`normalise_input`; timestamps are tz-aware UTC.
    """
    services = list(raw_services)
    planner = _Planner(
        services=services,
        min_transfer_seconds=min_transfer_seconds,
        remaining={
            (svc.service_id, i): svc.capacity
            for svc in services
            for i in range(len(svc.stops) - 1)
        },
    )
    served = set(planner.calling)

    ordered = sorted(raw_groups, key=lambda g: (g.ready_at, g.group_id))
    results: list[dict] = []
    assigned_count = 0
    for group in ordered:
        base = {
            "group_id": group.group_id,
            "origin": group.origin,
            "destination": group.destination,
            "ready_at": iso_str(group.ready_at),
            "party_size": group.party_size,
        }
        if group.origin not in served:
            results.append({
                **base,
                "status": "stranded",
                "arrival_at": None,
                "transfers": None,
                "legs": [],
                "stranded_reason": f"{ORIGIN_NOT_SERVED}: {_ORIGIN_MESSAGE}",
            })
            continue
        if group.destination not in served:
            results.append({
                **base,
                "status": "stranded",
                "arrival_at": None,
                "transfers": None,
                "legs": [],
                "stranded_reason": f"{DESTINATION_NOT_SERVED}: {_DESTINATION_MESSAGE}",
            })
            continue

        assignment = planner.search(group, enforce_capacity=True)
        if assignment is None:
            # Distinguish a network impossibility from a capacity obstruction:
            # rerun the search without capacity checks. If a path exists then,
            # capacity is the binding reason.
            unconstrained = planner.search(group, enforce_capacity=False)
            reason = (
                f"{INSUFFICIENT_CAPACITY}: {_CAPACITY_MESSAGE}"
                if unconstrained is not None
                else f"{UNREACHABLE_DESTINATION}: {_UNREACHABLE_MESSAGE}"
            )
            results.append({
                **base,
                "status": "stranded",
                "arrival_at": None,
                "transfers": None,
                "legs": [],
                "stranded_reason": reason,
            })
            continue

        planner.consume(assignment, group.party_size)
        assigned_count += 1
        results.append({
            **base,
            "status": "assigned",
            "arrival_at": iso_str(assignment.arrival_at),
            "transfers": assignment.transfers,
            "legs": [dict(leg) for leg in assignment.legs],
            "stranded_reason": None,
        })

    return {
        "min_transfer_seconds": min_transfer_seconds,
        "summary": {
            "group_count": len(ordered),
            "assigned_count": assigned_count,
            "stranded_count": len(ordered) - assigned_count,
        },
        "groups": results,
    }


def normalise_input(body) -> tuple[int, list[Service], list[Group]]:
    """Parse the request body into UTC-normalised dataclasses. Validation of
    station uniqueness and timetable monotonicity happens here too; callers map
    a returned ``ValueError`` onto HTTP 422 before any persistence."""
    services: list[Service] = []
    seen_services: set[str] = set()
    for svc in body.services:
        if svc.service_id in seen_services:
            raise ValueError(f"duplicate service_id '{svc.service_id}'")
        seen_services.add(svc.service_id)

        stops: list[Stop] = []
        seen_stations: set[str] = set()
        previous_departure: datetime | None = None
        for pos, stop in enumerate(svc.stops):
            if stop.station_id in seen_stations:
                raise ValueError(
                    f"service '{svc.service_id}' calls at station"
                    f" '{stop.station_id}' more than once; every stop must be unique"
                )
            seen_stations.add(stop.station_id)
            arrival = normalize_ts(stop.arrival_at)
            departure = normalize_ts(stop.departure_at)
            if departure < arrival:
                raise ValueError(
                    f"service '{svc.service_id}' stop {pos} ('{stop.station_id}'):"
                    " departure_at must not be before arrival_at"
                )
            if previous_departure is not None and arrival <= previous_departure:
                raise ValueError(
                    f"service '{svc.service_id}' is not monotonic at stop {pos}"
                    f" ('{stop.station_id}'): arrival_at must be strictly after the"
                    " previous stop's departure_at"
                )
            previous_departure = departure
            stops.append(Stop(
                station_id=stop.station_id,
                arrival_at=arrival,
                departure_at=departure,
            ))
        services.append(Service(
            service_id=svc.service_id,
            capacity=svc.capacity,
            stops=tuple(stops),
        ))

    groups: list[Group] = []
    seen_groups: set[str] = set()
    for grp in body.groups:
        if grp.group_id in seen_groups:
            raise ValueError(f"duplicate group_id '{grp.group_id}'")
        seen_groups.add(grp.group_id)
        groups.append(Group(
            group_id=grp.group_id,
            origin=grp.origin,
            destination=grp.destination,
            ready_at=normalize_ts(grp.ready_at),
            party_size=grp.party_size,
        ))

    return body.min_transfer_seconds, services, groups


def canonical_replan_payload(
    replan_id: str,
    min_transfer_seconds: int,
    services: list[Service],
    groups: list[Group],
) -> dict:
    """Canonical request: timestamps normalised to UTC, services sorted by
    service_id and groups sorted by (ready_at, group_id), so semantically
    identical submissions hash identically regardless of array order or offset
    notation. The stop order within a service is preserved because it defines
    the service's route."""
    return {
        "replan_id": replan_id,
        "min_transfer_seconds": min_transfer_seconds,
        "services": [
            {
                "service_id": svc.service_id,
                "capacity": svc.capacity,
                "stops": [
                    {
                        "station_id": stop.station_id,
                        "arrival_at": iso_str(stop.arrival_at),
                        "departure_at": iso_str(stop.departure_at),
                    }
                    for stop in svc.stops
                ],
            }
            for svc in sorted(services, key=lambda s: s.service_id)
        ],
        "groups": [
            {
                "group_id": grp.group_id,
                "origin": grp.origin,
                "destination": grp.destination,
                "ready_at": iso_str(grp.ready_at),
                "party_size": grp.party_size,
            }
            for grp in sorted(groups, key=lambda g: (iso_str(g.ready_at), g.group_id))
        ],
    }


def payload_hash(payload: dict) -> str:
    return hashlib.sha256(
        canonical_json(payload).encode("utf-8")
    ).hexdigest()
