"""Anonymous trajectory exports with k-anonymity suppression.

An export is a *read-only* analysis of stored paired trips, with results
written to isolated, append-only tables (``privacy_exports`` /
``privacy_export_trips``). It never mutates events, trips, trip charges,
disputes or the audit log.

Trips whose tap-in time (``started_at``) falls in the half-open UTC window
``[window_start, window_end)`` are grouped into equivalence classes keyed by

    (UTC time bucket, origin station group, destination station group).

Time buckets are fixed-width, epoch-anchored intervals (the bucket containing
instant ``t`` starts at ``floor(t / bucket_minutes) * bucket_minutes`` counted
from the Unix epoch in UTC), so bucket boundaries are independent of the
requested window.

Each passenger identifier is replaced by a stable HMAC-SHA256 pseudonym keyed
by the server-side ``PRIVACY_SALT``: the same real ``passenger_id`` maps to
the same pseudonym across every trip of the export (and across exports on the
same server), while the raw identifier never leaves the service.

For every equivalence class the *distinct* pseudonymised passengers are
counted. A class with fewer than ``k`` distinct passengers is suppressed as a
whole -- none of its trips are exported, so no single row can leak a member of
an under-populated class. Only trips in classes with at least ``k`` distinct
passengers are returned, carrying just the pseudonym, the bucket start and the
station groups.
"""
from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone

from .audit import canonical_json
from .settlement import iso_str, normalize_ts


class UnmappedStationsError(ValueError):
    """Raised when an in-window trip touches a station absent from the
    client-supplied station-to-group map."""

    def __init__(self, stations: set[str]):
        self.stations = sorted(stations)
        super().__init__(
            f"station_groups has no mapping for in-window stations: {self.stations}"
        )


def pseudonymize(passenger_id: str, salt: str) -> str:
    """Stable HMAC-SHA256 pseudonym for one passenger id.

    Deterministic for a given (salt, passenger_id): the same passenger keeps
    one pseudonym across every retained trip, and the raw id is never
    recoverable without the server-side salt.
    """
    return hmac.new(
        salt.encode("utf-8"),
        passenger_id.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def bucket_for(ts: datetime, bucket_minutes: int) -> datetime:
    """Start of the epoch-anchored UTC bucket containing ``ts``."""
    width_seconds = bucket_minutes * 60
    seconds = int(normalize_ts(ts).timestamp())
    floored = (seconds // width_seconds) * width_seconds
    return datetime.fromtimestamp(floored, tz=timezone.utc)


def load_window_trips(
    conn,
    window_start: datetime,
    window_end: datetime,
) -> list[dict]:
    """Load paired trips whose tap-in time is inside the half-open window.

    The window filter runs in Python (not on the stored ISO strings): optional
    fractional seconds would break lexicographic comparisons at the boundary,
    mirroring the reconciliation snapshot and quality scans.
    """
    rows = conn.execute(
        "SELECT trip_id, passenger_id, from_station, to_station, started_at"
        " FROM trips ORDER BY started_at, trip_id"
    ).fetchall()
    trips: list[dict] = []
    for r in rows:
        started = normalize_ts(
            datetime.fromisoformat(r["started_at"].replace("Z", "+00:00"))
        )
        if window_start <= started < window_end:
            trips.append(
                {
                    "trip_id": r["trip_id"],
                    "passenger_id": r["passenger_id"],
                    "from_station": r["from_station"],
                    "to_station": r["to_station"],
                    "started_at": started,
                }
            )
    return trips


@dataclass
class _Class:
    passengers: set[str] = field(default_factory=set)
    trips: list[dict] = field(default_factory=list)


def build_export(
    trips: list[dict],
    station_groups: dict[str, str],
    bucket_minutes: int,
    k: int,
    salt: str,
) -> dict:
    """Pseudonymise trips and apply whole-class k-anonymity suppression.

    Returns a result dict with aggregate counts, the retained equivalence
    classes (with their distinct-passenger counts) and the retained trip
    records. Raises ``UnmappedStationsError`` if any in-window trip endpoint
    is missing from ``station_groups``.
    """
    unmapped: set[str] = set()
    for t in trips:
        if t["from_station"] not in station_groups:
            unmapped.add(t["from_station"])
        if t["to_station"] not in station_groups:
            unmapped.add(t["to_station"])
    if unmapped:
        raise UnmappedStationsError(unmapped)

    classes: dict[tuple[str, str, str], _Class] = {}
    for t in trips:
        bucket = iso_str(bucket_for(t["started_at"], bucket_minutes))
        key = (
            bucket,
            station_groups[t["from_station"]],
            station_groups[t["to_station"]],
        )
        cls = classes.setdefault(key, _Class())
        cls.passengers.add(t["passenger_id"])
        cls.trips.append(t)

    # Whole-class suppression: a class with fewer than k DISTINCT passengers
    # drops every one of its trips. Partial suppression would leave a row that
    # itself discloses membership of the small class.
    retained_keys = {
        key for key, cls in classes.items() if len(cls.passengers) >= k
    }

    class_rows: list[dict] = []
    records: list[dict] = []
    for key in sorted(classes):
        cls = classes[key]
        if key not in retained_keys:
            continue
        class_rows.append(
            {
                "bucket_start": key[0],
                "origin_group": key[1],
                "destination_group": key[2],
                "passenger_count": len(cls.passengers),
                "trip_count": len(cls.trips),
            }
        )
        for t in cls.trips:
            records.append(
                {
                    "pseudonym": pseudonymize(t["passenger_id"], salt),
                    "bucket_start": key[0],
                    "origin_group": key[1],
                    "destination_group": key[2],
                    # Sorting tie-breakers only; stripped before persistence.
                    "_started_at": iso_str(t["started_at"]),
                    "_trip_id": t["trip_id"],
                }
            )

    records.sort(
        key=lambda r: (
            r["bucket_start"],
            r["origin_group"],
            r["destination_group"],
            r["pseudonym"],
            r["_started_at"],
            r["_trip_id"],
        )
    )
    for r in records:
        del r["_started_at"]
        del r["_trip_id"]

    retained_trip_count = len(records)
    return {
        "input_trip_count": len(trips),
        "equivalence_class_count": len(classes),
        "retained_class_count": len(retained_keys),
        "suppressed_class_count": len(classes) - len(retained_keys),
        "retained_trip_count": retained_trip_count,
        "suppressed_trip_count": len(trips) - retained_trip_count,
        "classes": class_rows,
        "trips": records,
    }


def canonical_export_payload(
    export_id: str,
    window_start: datetime,
    window_end: datetime,
    bucket_minutes: int,
    k: int,
    station_groups: dict[str, str],
) -> dict:
    """Canonical request: timestamps normalised to UTC and the station-to-group
    map sorted by station id, so semantically identical submissions hash
    identically regardless of object order or offset notation.

    The server-side salt is deliberately excluded: replay identity is a
    property of the request, while the frozen pseudonyms are stored verbatim.
    """
    return {
        "export_id": export_id,
        "window_start": iso_str(window_start),
        "window_end": iso_str(window_end),
        "bucket_minutes": bucket_minutes,
        "k": k,
        "station_groups": {
            station: group
            for station, group in sorted(station_groups.items())
        },
    }


def payload_hash(payload: dict) -> str:
    return hashlib.sha256(
        canonical_json(payload).encode("utf-8")
    ).hexdigest()


def classes_from_trip_rows(trip_rows) -> list[dict]:
    """Reconstruct the retained equivalence-class summary from stored trip
    rows (a pseudonym is injective per passenger within one export, so
    distinct pseudonyms equal distinct passengers)."""
    aggregated: dict[tuple[str, str, str], dict] = {}
    for r in trip_rows:
        key = (r["bucket_start"], r["origin_group"], r["destination_group"])
        cls = aggregated.setdefault(key, {"pseudonyms": set(), "trip_count": 0})
        cls["pseudonyms"].add(r["pseudonym"])
        cls["trip_count"] += 1
    return [
        {
            "bucket_start": key[0],
            "origin_group": key[1],
            "destination_group": key[2],
            "passenger_count": len(aggregated[key]["pseudonyms"]),
            "trip_count": aggregated[key]["trip_count"],
        }
        for key in sorted(aggregated)
    ]


def stored_groups_json(station_groups: dict[str, str]) -> str:
    return canonical_json(dict(sorted(station_groups.items())))


def load_station_groups(raw: str) -> dict[str, str]:
    return json.loads(raw)
