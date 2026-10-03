"""Settlement engine: pairs tap events into trips and prices them with
zone fares, transfer discounts and UTC daily / weekly (ISO, Monday-start) caps.

The engine is deterministic: for a given passenger it recomputes the full
history from the stored events, so late or corrected records simply change
the inputs and the diff against the previously persisted charges yields the
adjustment amounts."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from .config import FareConfig


@dataclass
class Event:
    event_id: str
    passenger_id: str
    station_id: str
    tap_type: str  # "in" | "out"
    occurred_at: datetime  # tz-aware UTC
    seq: int


@dataclass
class Pending:
    event: Event
    reason: str


@dataclass
class Trip:
    trip_id: str
    passenger_id: str
    in_event: Event
    out_event: Event
    from_zone: int
    to_zone: int
    base_fare_cents: int = 0
    transfer_discount_cents: int = 0
    amount_cents: int = 0
    basis: dict = None  # type: ignore[assignment]

    @property
    def day_key(self) -> str:
        return self.in_event.occurred_at.date().isoformat()

    @property
    def week_key(self) -> str:
        iso = self.in_event.occurred_at.date().isocalendar()
        return f"{iso[0]}-W{iso[1]:02d}"


def normalize_ts(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def iso_str(ts: datetime) -> str:
    return normalize_ts(ts).isoformat().replace("+00:00", "Z")


def pair_trips(events: list[Event]) -> tuple[list[Trip], list[Pending]]:
    """Scan events in (occurred_at, seq) order. A tap-in opens a trip, the next
    tap-out closes it. A tap-out with no open tap-in, a tap-in superseded by
    another tap-in, and a tap-in never closed all become pending records."""
    ordered = sorted(events, key=lambda e: (e.occurred_at, e.seq, e.event_id))
    trips: list[Trip] = []
    pending: list[Pending] = []
    open_in: Event | None = None
    for ev in ordered:
        if ev.tap_type == "in":
            if open_in is not None:
                pending.append(Pending(open_in, "unclosed_tap_in"))
            open_in = ev
        else:
            if open_in is None:
                pending.append(Pending(ev, "tap_out_without_tap_in"))
            else:
                trips.append(
                    Trip(
                        trip_id=f"{open_in.event_id}:{ev.event_id}",
                        passenger_id=ev.passenger_id,
                        in_event=open_in,
                        out_event=ev,
                        from_zone=0,
                        to_zone=0,
                    )
                )
                open_in = None
    if open_in is not None:
        pending.append(Pending(open_in, "missing_tap_out"))
    return trips, pending


def price_trips(trips: list[Trip], cfg: FareConfig) -> None:
    """Fill in zones, fares, transfer discounts and cap-limited amounts."""
    window = timedelta(minutes=cfg.transfer_window_minutes)
    day_spent: dict[str, int] = {}
    week_spent: dict[str, int] = {}
    prev: Trip | None = None

    for trip in trips:  # trips arrive in chronological order from pair_trips
        trip.from_zone = cfg.zone_of(trip.in_event.station_id)  # type: ignore[assignment]
        trip.to_zone = cfg.zone_of(trip.out_event.station_id)  # type: ignore[assignment]
        base = cfg.fare_between(trip.from_zone, trip.to_zone)
        trip.base_fare_cents = base

        discount = 0
        transfer = False
        if prev is not None:
            gap = trip.in_event.occurred_at - prev.out_event.occurred_at
            if timedelta(0) <= gap <= window:
                transfer = True
                discount = min(cfg.transfer_discount_cents, base)
        trip.transfer_discount_cents = discount

        fare = base - discount
        day_key, week_key = trip.day_key, trip.week_key
        day_before = day_spent.get(day_key, 0)
        week_before = week_spent.get(week_key, 0)
        day_room = max(0, cfg.daily_cap_cents - day_before)
        week_room = max(0, cfg.weekly_cap_cents - week_before)
        amount = max(0, min(fare, day_room, week_room))

        day_spent[day_key] = day_before + amount
        week_spent[week_key] = week_before + amount
        trip.amount_cents = amount
        trip.basis = {
            "zone_fare_rule": f"zone {trip.from_zone} -> zone {trip.to_zone}",
            "base_fare_cents": base,
            "transfer": {
                "applied": transfer,
                "window_minutes": cfg.transfer_window_minutes,
                "discount_cents": discount,
                "previous_trip_id": prev.trip_id if transfer else None,
            },
            "fare_after_discount_cents": fare,
            "daily_cap": {
                "day_key": day_key,
                "cap_cents": cfg.daily_cap_cents,
                "spent_before_cents": day_before,
                "applied": amount < fare and day_room <= week_room,
            },
            "weekly_cap": {
                "week_key": week_key,
                "cap_cents": cfg.weekly_cap_cents,
                "spent_before_cents": week_before,
                "applied": amount < fare and week_room < day_room,
            },
            "final_amount_cents": amount,
        }
        prev = trip


def trip_row(trip: Trip) -> tuple:
    return (
        trip.trip_id,
        trip.passenger_id,
        trip.in_event.event_id,
        trip.out_event.event_id,
        trip.in_event.station_id,
        trip.out_event.station_id,
        trip.from_zone,
        trip.to_zone,
        iso_str(trip.in_event.occurred_at),
        iso_str(trip.out_event.occurred_at),
        trip.day_key,
        trip.week_key,
    )


def charge_row(trip: Trip) -> tuple:
    return (
        trip.trip_id,
        trip.passenger_id,
        trip.day_key,
        trip.week_key,
        trip.base_fare_cents,
        trip.transfer_discount_cents,
        trip.amount_cents,
        json.dumps(trip.basis, ensure_ascii=False),
    )


def build_charge_adjustments(
    trips: list[Trip], old_charges: dict[str, int]
) -> list[dict]:
    """Diff newly priced trips against the previously persisted per-trip
    amounts. Trips whose id disappeared after re-pairing show up as negative
    reversals with a ``voided`` basis. Pure: it neither reads nor writes."""
    new_charges = {t.trip_id: t.amount_cents for t in trips}

    adjustments: list[dict] = []
    for trip in trips:
        old = old_charges.get(trip.trip_id)
        new = trip.amount_cents
        if old != new:
            adjustments.append(
                {
                    "trip_id": trip.trip_id,
                    "day_key": trip.day_key,
                    "week_key": trip.week_key,
                    "old_amount_cents": old if old is not None else 0,
                    "new_amount_cents": new,
                    "delta_cents": new - (old if old is not None else 0),
                    "basis": trip.basis,
                }
            )
    for trip_id, old in old_charges.items():
        if trip_id not in new_charges:
            adjustments.append(
                {
                    "trip_id": trip_id,
                    "day_key": None,
                    "week_key": None,
                    "old_amount_cents": old,
                    "new_amount_cents": 0,
                    "delta_cents": -old,
                    "basis": {"voided": True, "reason": "trip no longer exists after re-pairing"},
                }
            )
    return adjustments


def simulate_passenger(
    events: list[Event], old_charges: dict[str, int], cfg: FareConfig
) -> tuple[list[Trip], list[Pending], list[dict]]:
    """Pair and price a hypothetical event list without persisting anything.
    Returns the priced trips, the pending events and the charge adjustments
    against ``old_charges``. Used by the correction impact preview, which must
    run the exact settlement rules in memory and never touch stored data."""
    trips, pending = pair_trips(events)
    price_trips(trips, cfg)
    adjustments = build_charge_adjustments(trips, old_charges)
    return trips, pending, adjustments


def settle_passenger(conn, cfg: FareConfig, passenger_id: str) -> list[dict]:
    """Recompute a passenger's full settlement, persist it, and return the
    list of adjustments (per-trip old/new amounts and billing basis).

    Events frozen under a review hold (``held_id IS NOT NULL``) are excluded:
    while held they take no part in pairing or billing, and their stored
    pairing status is left untouched until the hold is released."""
    rows = conn.execute(
        "SELECT * FROM events WHERE passenger_id = ? AND held_id IS NULL"
        " ORDER BY occurred_at, seq",
        (passenger_id,),
    ).fetchall()
    events = [
        Event(
            event_id=r["event_id"],
            passenger_id=r["passenger_id"],
            station_id=r["station_id"],
            tap_type=r["tap_type"],
            occurred_at=normalize_ts(datetime.fromisoformat(r["occurred_at"].replace("Z", "+00:00"))),
            seq=r["seq"],
        )
        for r in rows
    ]

    trips, pending = pair_trips(events)
    price_trips(trips, cfg)

    old_charges = {
        r["trip_id"]: r["amount_cents"]
        for r in conn.execute(
            "SELECT trip_id, amount_cents FROM trip_charges WHERE passenger_id = ?",
            (passenger_id,),
        )
    }
    adjustments = build_charge_adjustments(trips, old_charges)

    paired_ids = {t.in_event.event_id for t in trips} | {t.out_event.event_id for t in trips}
    pending_by_id = {p.event.event_id: p.reason for p in pending}

    conn.execute("DELETE FROM trips WHERE passenger_id = ?", (passenger_id,))
    conn.execute("DELETE FROM trip_charges WHERE passenger_id = ?", (passenger_id,))
    conn.executemany(
        "INSERT INTO trips (trip_id, passenger_id, in_event_id, out_event_id,"
        " from_station, to_station, from_zone, to_zone, started_at, ended_at,"
        " day_key, week_key) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        [trip_row(t) for t in trips],
    )
    conn.executemany(
        "INSERT INTO trip_charges (trip_id, passenger_id, day_key, week_key,"
        " base_fare_cents, transfer_discount_cents, amount_cents, basis)"
        " VALUES (?,?,?,?,?,?,?,?)",
        [charge_row(t) for t in trips],
    )
    conn.executemany(
        "UPDATE events SET status = 'paired', pending_reason = NULL"
        " WHERE passenger_id = ? AND event_id = ?",
        [(passenger_id, eid) for eid in paired_ids],
    )
    conn.executemany(
        "UPDATE events SET status = 'pending', pending_reason = ?"
        " WHERE passenger_id = ? AND event_id = ?",
        [(reason, passenger_id, eid) for eid, reason in pending_by_id.items()],
    )
    return adjustments
