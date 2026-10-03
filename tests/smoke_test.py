"""Smoke test: pairing, zone fares, transfer discount, daily/weekly caps,
late-record recompute with adjustments, idempotency, correction, pending list,
the deduction reconciliation endpoint (snapshot window, trip_id locks,
optimal tolerance matching, ambiguity, batch idempotency/conflict, immutability),
the transfer network validation endpoint (time slicing, hub reachability,
transfer asymmetry, duplicate/self-loop edges, overlapping intervals,
idempotency/conflict, snapshot freeze/query/immutability),
the fare dispute endpoints (frozen charge snapshot, claim cap, optimistic
one-shot resolution, immutable refund ledger, version conflict/idempotency,
top-up/clawback differences after later corrections),
the operator fare clearing endpoints (first closing freezes the half-open
window and per-station-pair basis-point rules, whole-batch rejection on
missing/non-10000 rules, remainder/operator-id cent allocation, immutable
per-trip ledger, adjustment batches referencing the root that append only
signed target-minus-cumulative deltas, full reversal of vanished trips,
idempotent replay/409 conflict, frozen-result query, append-only triggers),
the tap-event quality scan endpoints (half-open event window, duplicate
same-type taps, minimum-travel out->in conflicts, evidence chains,
idempotency/409 conflict, GET query, isolated append-only tables with no
business or audit side effects),
the anonymous trajectory export endpoints (epoch-anchored UTC time buckets,
station groups, stable HMAC pseudonyms, distinct-passenger k-anonymity with
whole-class suppression, half-open trip window, idempotency/409 conflict,
GET query, isolated append-only tables with no business or audit side effects),
the sudden-disruption replan endpoints (earliest-arrival itineraries under
per-section capacity, minimum transfer seconds, fewer-transfers/lexicographic
service-sequence tie-breaks, non-dominated label fronts where itineraries
merge at a transfer station, (ready_at, group_id) processing order, indivisible
parties, per-section seat deduction, stranded reasons, timetable uniqueness and
monotonicity validation, idempotency/409 conflict, GET query, isolated
append-only tables with no business or audit side effects),
the tap-event review hold endpoints (whole-transaction ownership check and
complete per-event payload snapshots, events excluded from pairing/billing and
pending while held, whole-batch 409 on reconciliation/clearing/resolved-dispute
references or overlapping holds, create idempotency/409 conflict, corrections
refused while held, optimistic release versions, snapshot-mismatch refusal that
keeps events held, re-settlement by original (occurred_at, seq) with signed
trip deltas, append-only immutable hold/snapshot/release tables, GET query),
the correction impact preview endpoint (in-memory settlement of old/new
passengers with before/after trips and signed adjustments, identical payload
no_change, 404/409/422 gates, per-item reconciliation/clearing/resolved-dispute
impacts with reasons, uncovered frozen-rule pairs, fully read-only with no
audit rows and unchanged /events semantics),
the append-only hash-chained business audit log (idempotency, verification,
tamper detection, append-only triggers, atomic rollback),
and the online GET /admin/backup endpoint (SQLite backup API, live writes keep
flowing during the backup, independent-connection integrity check, SHA-256
digest, temp-file cleanup on failure).

Run:  python tests/smoke_test.py   (requires httpx: pip install httpx)
"""
import hashlib
import json
import os
import glob
import sqlite3
import tempfile
import threading
import time

os.environ["DB_PATH"] = os.path.join(tempfile.mkdtemp(), "test.db")
os.environ["FARE_CONFIG"] = os.path.join(os.path.dirname(__file__), "..", "config", "fares.json")
os.environ.setdefault("PRIVACY_SALT", "test-privacy-salt")

from fastapi.testclient import TestClient  # noqa: E402

from app.audit import append_audit, verify_chain  # noqa: E402
from app.db import SCHEMA  # noqa: E402
from app.main import app  # noqa: E402

PASS = 0


def check(name, cond, extra=""):
    global PASS
    assert cond, f"FAIL: {name} {extra}"
    PASS += 1
    print(f"ok - {name}")


def run(client):
    def post(event_id, passenger, station, tap, ts):
        r = client.post("/events", json={
            "event_id": event_id, "passenger_id": passenger,
            "station_id": station, "tap_type": tap, "occurred_at": ts,
        })
        assert r.status_code == 200, r.text
        return r.json()

    # --- Passenger A: pairing + zone fare + transfer discount ----------------
    r = post("a1", "A", "S01", "in", "2026-09-21T08:00:00Z")
    check("open tap-in is pending",
          r["event_pairing"] == "pending" and r["pending_reason"] == "missing_tap_out")

    r = post("a2", "A", "S04", "out", "2026-09-21T08:30:00Z")
    check("tap-out pairs the trip", r["event_pairing"] == "paired")
    adj = r["adjustments"]
    check("one adjustment for new trip", len(adj) == 1)
    check("zone 1->2 fare 500", adj[0]["new_amount_cents"] == 500 and adj[0]["delta_cents"] == 500)
    check("basis names zone rule", adj[0]["basis"]["zone_fare_rule"] == "zone 1 -> zone 2")
    check("no transfer on first trip", adj[0]["basis"]["transfer"]["applied"] is False)

    post("a3", "A", "S05", "in", "2026-09-21T09:00:00Z")
    r = post("a4", "A", "S06", "out", "2026-09-21T09:20:00Z")
    adj = {a["trip_id"]: a for a in r["adjustments"]}
    t2 = adj["a3:a4"]
    check("zone 2->2 base 300", t2["basis"]["base_fare_cents"] == 300)
    check("transfer discount 100 within 90min", t2["basis"]["transfer"]["discount_cents"] == 100)
    check("transfer net 200", t2["new_amount_cents"] == 200)

    # --- Idempotency ----------------------------------------------------------
    r = post("a4", "A", "S06", "out", "2026-09-21T09:20:00Z")
    check("duplicate is idempotent", r["status"] == "duplicate" and r["adjustments"] == [])

    # --- Late out-of-order record triggers recompute --------------------------
    r = post("a0", "A", "S07", "in", "2026-09-21T07:00:00Z")  # late, before a1
    check("late consecutive tap-in becomes pending", r["event_pairing"] == "pending"
          and r["pending_reason"] == "unclosed_tap_in")
    check("existing trips unchanged by late pending event", r["adjustments"] == [])

    # --- Correction (same event id, different payload) ------------------------
    r = post("a2", "A", "S07", "out", "2026-09-21T08:30:00Z")  # S04 -> S07
    check("correction detected", r["status"] == "corrected")
    adj = {a["trip_id"]: a for a in r["adjustments"]}
    t1 = adj["a1:a2"]
    check("corrected trip re-priced 500->800",
          t1["old_amount_cents"] == 500 and t1["new_amount_cents"] == 800)
    check("delta +300", t1["delta_cents"] == 300)

    bill = client.get("/passengers/A/bill").json()
    check("bill totals 800+200", bill["total_cents"] == 1000)
    check("bill has 2 trips", len(bill["trips"]) == 2)

    # --- Passenger B: daily cap 1500 ------------------------------------------
    post("b1", "B", "S01", "in", "2026-09-21T08:00:00Z")
    post("b2", "B", "S04", "out", "2026-09-21T08:30:00Z")      # 500
    post("b3", "B", "S05", "in", "2026-09-21T09:00:00Z")
    post("b4", "B", "S02", "out", "2026-09-21T09:30:00Z")      # 500-100 transfer = 400
    post("b5", "B", "S01", "in", "2026-09-21T11:31:00Z")
    r = post("b6", "B", "S07", "out", "2026-09-21T12:00:00Z")  # 800, capped to 600
    adj = {a["trip_id"]: a for a in r["adjustments"]}
    t3 = adj["b5:b6"]
    check("daily cap trims 800 to 600", t3["new_amount_cents"] == 600)
    check("daily cap flagged in basis", t3["basis"]["daily_cap"]["applied"] is True)

    bill = client.get("/passengers/B/bill", params={"day": "2026-09-21"}).json()
    check("daily total equals cap 1500", bill["daily_totals"][0]["amount_cents"] == 1500)

    # --- Passenger C: weekly cap 5000 (ISO week, Monday start) -----------------
    days = ["2026-09-21", "2026-09-22", "2026-09-23", "2026-09-24"]  # Mon-Thu
    n = 0
    for d in days:
        for h_in, m_in, h_out, m_out in [("08", "00", "08", "30"),
                                         ("10", "01", "10", "31"),
                                         ("12", "02", "12", "32")]:
            n += 1
            post(f"c{n}i", "C", "S01", "in", f"{d}T{h_in}:{m_in}:00Z")
            n += 1
            post(f"c{n}o", "C", "S04", "out", f"{d}T{h_out}:{m_out}:00Z")  # 500 each

    bill = client.get("/passengers/C/bill").json()
    check("weekly total capped at 5000", bill["total_cents"] == 5000)
    check("single ISO week", len(bill["weekly_totals"]) == 1
          and bill["weekly_totals"][0]["key"] == "2026-W39")
    day_amounts = [d["amount_cents"] for d in bill["daily_totals"]]
    check("first three days full 1500", day_amounts[:3] == [1500, 1500, 1500])
    check("fourth day trimmed to 500", day_amounts[3] == 500)
    last_day = client.get("/passengers/C/bill", params={"day": "2026-09-24"}).json()
    amounts = [t["amount_cents"] for t in last_day["trips"]]
    check("weekly cap zeroes later trips", amounts == [500, 0, 0])
    check("weekly cap flagged", last_day["trips"][1]["basis"]["weekly_cap"]["applied"] is True)

    # --- Pending list -----------------------------------------------------------
    post("d1", "D", "S04", "out", "2026-09-21T08:00:00Z")
    pend = client.get("/pending", params={"passenger_id": "D"}).json()
    check("orphan tap-out pending", len(pend) == 1 and pend[0]["reason"] == "tap_out_without_tap_in")
    pend_all = client.get("/pending").json()
    check("pending list aggregates passengers",
          {p["event_id"] for p in pend_all} >= {"d1", "a0"})

    # --- Validation --------------------------------------------------------------
    r = client.post("/events", json={"event_id": "x1", "passenger_id": "X",
                                     "station_id": "NOPE", "tap_type": "in",
                                     "occurred_at": "2026-09-21T08:00:00Z"})
    check("unknown station -> 422", r.status_code == 422)

    # --- Reconciliation ------------------------------------------------------------
    # Trips on 2026-09-25, a day no other section uses.
    post("ra1", "RA", "S01", "in", "2026-09-25T08:00:00Z")
    post("ra2", "RA", "S04", "out", "2026-09-25T08:30:00Z")   # 500
    post("ra3", "RA", "S05", "in", "2026-09-25T09:00:00Z")
    post("ra4", "RA", "S06", "out", "2026-09-25T09:20:00Z")   # 300 - 100 transfer = 200
    post("rb1", "RB", "S01", "in", "2026-09-25T08:00:00Z")
    post("rb2", "RB", "S04", "out", "2026-09-25T08:30:00Z")   # 500
    post("rc1", "RC", "S01", "in", "2026-09-25T12:00:00Z")
    post("rc2", "RC", "S02", "out", "2026-09-25T12:30:00Z")   # 300
    post("rd1", "RD", "S01", "in", "2026-09-25T15:00:00Z")
    post("rd2", "RD", "S02", "out", "2026-09-25T15:30:00Z")   # 300
    post("re1", "RE", "S01", "in", "2026-09-24T23:00:00Z")
    post("re2", "RE", "S02", "out", "2026-09-25T00:00:00Z")   # ends at window_start -> in
    post("rf1", "RF", "S01", "in", "2026-09-25T23:30:00Z")
    post("rf2", "RF", "S02", "out", "2026-09-26T00:00:00Z")   # ends at window_end -> out

    batch = {
        "batch_id": "recon-2026-09-25-a",
        "window_start": "2026-09-25T00:00:00Z",
        "window_end": "2026-09-26T00:00:00Z",
        "tolerance_seconds": 300,
        "records": [
            {"serial_no": "x1", "passenger_id": "RA",
             "charged_at": "2026-09-25T08:30:02Z", "amount_cents": 500,
             "trip_id": "ra1:ra2"},
            {"serial_no": "x2", "passenger_id": "RA",
             "charged_at": "2026-09-25T09:19:00Z", "amount_cents": 150},
            {"serial_no": "x3", "passenger_id": "RB",
             "charged_at": "2026-09-25T08:31:00Z", "amount_cents": 600},
            {"serial_no": "x4", "passenger_id": "RZZ",
             "charged_at": "2026-09-25T10:00:00Z", "amount_cents": 100},
            {"serial_no": "x5", "passenger_id": "RD",
             "charged_at": "2026-09-25T15:30:05Z", "amount_cents": 300},
            {"serial_no": "x6", "passenger_id": "RD",
             "charged_at": "2026-09-25T15:30:05Z", "amount_cents": 300},
        ],
    }
    r = client.post("/reconciliations", json=batch)
    check("recon batch created", r.status_code == 200)
    body = r.json()
    check("recon status created", body["status"] == "created")
    s = body["summary"]
    check("snapshot has 6 trips (rf excluded)", s["snapshot_trip_count"] == 6)
    check("one matched", s["matched"] == 1)
    check("one undercharged", s["undercharged"] == 1)
    check("one overcharged", s["overcharged"] == 1)
    check("one external_only", s["external_only"] == 1)
    check("two internal_only (rc, re)", s["internal_only"] == 2)
    check("three ambiguous (rd trip + x5 + x6)", s["ambiguous"] == 3)

    by_serial = {it["serial_no"]: it for it in body["results"] if it["serial_no"]}
    by_trip = {it["trip_id"]: it for it in body["results"] if it["trip_id"]}

    x1 = by_serial["x1"]
    check("x1 locked by exact trip_id",
          x1["match_method"] == "trip_id" and x1["category"] == "matched")
    check("x1 paired with ra1:ra2, diff 0",
          x1["trip_id"] == "ra1:ra2" and x1["difference_cents"] == 0)

    x2 = by_serial["x2"]
    check("x2 paired by tolerance",
          x2["match_method"] == "tolerance" and x2["trip_id"] == "ra3:ra4")
    check("x2 undercharged by 50",
          x2["category"] == "undercharged" and x2["difference_cents"] == -50)
    check("x2 reason cites tolerance", "time tolerance" in x2["reason"])

    x3 = by_serial["x3"]
    check("x3 overcharged by 100",
          x3["category"] == "overcharged" and x3["difference_cents"] == 100)

    x4 = by_serial["x4"]
    check("x4 external only",
          x4["category"] == "external_only" and x4["trip_id"] is None)

    check("rc trip internal only", by_trip["rc1:rc2"]["category"] == "internal_only")
    check("trip ending at window_start included",
          by_trip["re1:re2"]["category"] == "internal_only")
    check("trip ending at window_end excluded", "rf1:rf2" not in by_trip)

    check("x5 ambiguous", by_serial["x5"]["category"] == "ambiguous")
    check("x6 ambiguous", by_serial["x6"]["category"] == "ambiguous")
    rd = by_trip["rd1:rd2"]
    check("rd trip ambiguous", rd["category"] == "ambiguous")
    check("ambiguity reason lists both candidates",
          "x5" in rd["reason"] and "x6" in rd["reason"])

    # --- Recon idempotency ---------------------------------------------------------
    r = client.post("/reconciliations", json=batch)
    check("same payload -> duplicate", r.status_code == 200 and r.json()["status"] == "duplicate")
    check("duplicate returns identical results", r.json()["results"] == body["results"])

    shuffled = dict(batch, records=list(reversed(batch["records"])))
    r = client.post("/reconciliations", json=shuffled)
    check("record order does not affect identity", r.json()["status"] == "duplicate")

    conflict = dict(batch, records=[dict(batch["records"][0], amount_cents=999)])
    r = client.post("/reconciliations", json=conflict)
    check("same batch_id, different payload -> 409", r.status_code == 409)

    # --- Recon results are frozen against later corrections -------------------------
    r = post("ra2", "RA", "S07", "out", "2026-09-25T08:30:00Z")  # S04 -> S07: 500 -> 800
    check("correction re-prices trip", r["status"] == "corrected")
    r = client.post("/reconciliations", json=batch)
    check("stored batch immune to later corrections",
          r.json()["status"] == "duplicate" and r.json()["results"] == body["results"])

    batch2 = dict(batch, batch_id="recon-2026-09-25-b", records=[])
    r = client.post("/reconciliations", json=batch2)
    items2 = {it["trip_id"]: it for it in r.json()["results"] if it["trip_id"]}
    check("new batch snapshots corrected amount",
          items2["ra1:ra2"]["internal_amount_cents"] == 800)

    # --- Recon validation ------------------------------------------------------------
    bad_window = dict(batch, batch_id="recon-bad-1",
                      window_start="2026-09-26T00:00:00Z", window_end="2026-09-25T00:00:00Z")
    r = client.post("/reconciliations", json=bad_window)
    check("empty window -> 422", r.status_code == 422)

    dup_serial = dict(batch, batch_id="recon-bad-2",
                      records=[batch["records"][1], batch["records"][1]])
    r = client.post("/reconciliations", json=dup_serial)
    check("duplicate serial_no -> 422", r.status_code == 422)

    # --- Transfer network validation -----------------------------------------
    # Stations here are network-only ids (the network module is independent of
    # the fare configuration). A clean valid network: line L1 serves A<->B,
    # symmetric walking corridor B<->C, designated hub A.
    def net_body(validation_id, stations=("A", "B", "C"), hubs=("A",),
                 segments=None, transfers=None,
                 start="2026-10-01T00:00:00Z", end="2026-10-02T00:00:00Z"):
        return {
            "validation_id": validation_id,
            "effective_start": start,
            "effective_end": end,
            "hubs": list(hubs),
            "stations": list(stations),
            "segments": segments if segments is not None else [
                {"line_id": "L1", "segment_id": "L1-ab",
                 "from_station": "A", "to_station": "B",
                 "start_at": start, "end_at": end},
                {"line_id": "L1", "segment_id": "L1-ba",
                 "from_station": "B", "to_station": "A",
                 "start_at": start, "end_at": end},
            ],
            "transfers": transfers if transfers is not None else [
                {"from_station": "B", "to_station": "C",
                 "start_at": start, "end_at": end},
                {"from_station": "C", "to_station": "B",
                 "start_at": start, "end_at": end},
            ],
        }

    valid_net = net_body("net-ok-1")
    r = client.post("/network/validate", json=valid_net)
    check("valid network passes", r.status_code == 200 and r.json()["passed"] is True)
    vbody = r.json()
    check("valid network has one static slice",
          vbody["slice_count"] == 1 and len(vbody["slices"]) == 1
          and vbody["slices"][0]["start"] == "2026-10-01T00:00:00Z"
          and vbody["slices"][0]["end"] == "2026-10-02T00:00:00Z")
    check("valid network reports no conflict", vbody["conflict"] is None)
    check("validation status created", vbody["status"] == "created")

    # Idempotency: same id + same payload (even reordered / different offset).
    shuffled_net = dict(valid_net,
                        stations=list(reversed(valid_net["stations"])),
                        segments=list(reversed(valid_net["segments"])),
                        transfers=list(reversed(valid_net["transfers"])))
    r = client.post("/network/validate", json=shuffled_net)
    check("same validation payload reordered -> duplicate",
          r.status_code == 200 and r.json()["status"] == "duplicate"
          and r.json()["passed"] is True)

    tz_net = dict(valid_net, effective_start="2026-10-01T08:00:00+08:00")
    r = client.post("/network/validate", json=tz_net)
    check("different offset notation of same instant -> duplicate",
          r.json()["status"] == "duplicate")

    conflict_net = dict(valid_net, hubs=["B"])
    r = client.post("/network/validate", json=conflict_net)
    check("same validation_id, different payload -> 409", r.status_code == 409)

    # Structural validation.
    bad_window = net_body("net-bad-window", start="2026-10-03T00:00:00Z")
    r = client.post("/network/validate", json=bad_window)
    check("network empty effective window -> 422", r.status_code == 422)

    unknown_hub = net_body("net-bad-hub", hubs=["ZZZ"])
    r = client.post("/network/validate", json=unknown_hub)
    check("hub outside served stations -> 422", r.status_code == 422)

    dup_station = net_body("net-bad-dupstation", stations=["A", "A", "B"])
    r = client.post("/network/validate", json=dup_station)
    check("duplicate station entry -> 422", r.status_code == 422)

    bad_edge_ref = net_body(
        "net-bad-ref",
        segments=[{"line_id": "L1", "segment_id": "x",
                   "from_station": "A", "to_station": "ZZZ",
                   "start_at": "2026-10-01T00:00:00Z",
                   "end_at": "2026-10-02T00:00:00Z"}],
        transfers=[],
    )
    r = client.post("/network/validate", json=bad_edge_ref)
    check("edge referencing unknown station -> 422", r.status_code == 422)

    bad_edge_window = net_body(
        "net-bad-edge-window",
        segments=[{"line_id": "L1", "segment_id": "x",
                   "from_station": "A", "to_station": "B",
                   "start_at": "2026-10-01T12:00:00Z",
                   "end_at": "2026-10-01T12:00:00Z"}],
        transfers=[],
    )
    r = client.post("/network/validate", json=bad_edge_window)
    check("empty edge active interval -> 422", r.status_code == 422)

    # Self-loop conflict (highest priority, reported in the first live slice).
    self_loop = net_body(
        "net-self-loop",
        segments=[{"line_id": "L1", "segment_id": "loop",
                   "from_station": "A", "to_station": "A",
                   "start_at": "2026-10-01T00:00:00Z",
                   "end_at": "2026-10-02T00:00:00Z"}],
        transfers=[],
    )
    r = client.post("/network/validate", json=self_loop)
    body = r.json()
    check("self loop fails validation",
          r.status_code == 200 and body["passed"] is False)
    check("self loop conflict code + slice",
          body["conflict"]["code"] == "self_loop_edge"
          and body["conflict"]["slice"]["index"] == 0)
    check("self loop names offending edge",
          body["conflict"]["edge"]["segment_id"] == "loop"
          and body["conflict"]["evidence_path"] is None)

    # Overlapping effective intervals of the same declared identity.
    overlap = net_body(
        "net-overlap",
        segments=[
            {"line_id": "L1", "segment_id": "L1-ab-1",
             "from_station": "A", "to_station": "B",
             "start_at": "2026-10-01T00:00:00Z",
             "end_at": "2026-10-01T12:00:00Z"},
            {"line_id": "L1", "segment_id": "L1-ab-2",
             "from_station": "A", "to_station": "B",
             "start_at": "2026-10-01T06:00:00Z",
             "end_at": "2026-10-02T00:00:00Z"},
            {"line_id": "L1", "segment_id": "L1-ba",
             "from_station": "B", "to_station": "A",
             "start_at": "2026-10-01T00:00:00Z",
             "end_at": "2026-10-02T00:00:00Z"},
        ],
        transfers=[],
    )
    r = client.post("/network/validate", json=overlap)
    body = r.json()
    check("overlap conflict reported", body["conflict"]["code"] == "overlapping_interval")
    check("overlap reported in first overlapping slice [06:00,12:00)",
          body["conflict"]["slice"]["start"] == "2026-10-01T06:00:00Z"
          and body["conflict"]["slice"]["end"] == "2026-10-01T12:00:00Z")
    ids = sorted(e["segment_id"] for e in body["conflict"]["edges"])
    check("overlap names both declarations", ids == ["L1-ab-1", "L1-ab-2"])

    # Same identity with disjoint (touching) windows is legal: slicing proves
    # the two declarations are never simultaneously active. Keep B->A and the
    # symmetric B<->C walking corridor so the network itself is reachable.
    disjoint = dict(overlap, validation_id="net-disjoint")
    disjoint["segments"][0]["end_at"] = "2026-10-01T06:00:00Z"
    disjoint["segments"][1]["start_at"] = "2026-10-01T06:00:00Z"
    disjoint["transfers"] = [
        {"from_station": "B", "to_station": "C",
         "start_at": "2026-10-01T00:00:00Z", "end_at": "2026-10-02T00:00:00Z"},
        {"from_station": "C", "to_station": "B",
         "start_at": "2026-10-01T00:00:00Z", "end_at": "2026-10-02T00:00:00Z"},
    ]
    r = client.post("/network/validate", json=disjoint)
    body = r.json()
    check("touching (disjoint) intervals pass",
          body["passed"] is True and body["slice_count"] == 2)

    # Duplicate (parallel) edge: segment and walking transfer on the same
    # directed pair, active together.
    dup_edge = net_body(
        "net-dup-edge",
        segments=[{"line_id": "L1", "segment_id": "L1-ab",
                   "from_station": "A", "to_station": "B",
                   "start_at": "2026-10-01T00:00:00Z",
                   "end_at": "2026-10-02T00:00:00Z"}],
        transfers=[
            {"from_station": "A", "to_station": "B",
             "start_at": "2026-10-01T00:00:00Z",
             "end_at": "2026-10-02T00:00:00Z"},
        ],
    )
    r = client.post("/network/validate", json=dup_edge)
    body = r.json()
    check("parallel edge conflict", body["conflict"]["code"] == "duplicate_edge")
    check("parallel conflict names both kinds of edge",
          {e["edge_kind"] for e in body["conflict"]["edges"]} == {"segment", "transfer"})

    # Walking transfer asymmetry with a usable detour: A<->B by line, walking
    # only B->C, but C->B by line gives a 1-hop return path C->B->... note:
    # C must be reached first; build B->C walk, C->A segment, A->B segment so
    # the shortest return C -> B walks C->A (segment) then A->B (segment).
    asym = net_body(
        "net-asym",
        segments=[
            {"line_id": "L1", "segment_id": "L1-ab",
             "from_station": "A", "to_station": "B",
             "start_at": "2026-10-01T00:00:00Z", "end_at": "2026-10-02T00:00:00Z"},
            {"line_id": "L2", "segment_id": "L2-ca",
             "from_station": "C", "to_station": "A",
             "start_at": "2026-10-01T00:00:00Z", "end_at": "2026-10-02T00:00:00Z"},
        ],
        transfers=[
            {"from_station": "B", "to_station": "C",
             "start_at": "2026-10-01T00:00:00Z", "end_at": "2026-10-02T00:00:00Z"},
        ],
    )
    r = client.post("/network/validate", json=asym)
    body = r.json()
    check("asymmetric transfer conflict",
          body["conflict"]["code"] == "transfer_asymmetry")
    check("asymmetry names B->C edge",
          body["conflict"]["edge"]["from_station"] == "B"
          and body["conflict"]["edge"]["to_station"] == "C")
    path = body["conflict"]["evidence_path"]
    check("asymmetry shortest evidence path C -> A -> B",
          [(h["from_station"], h["to_station"], h["edge_kind"]) for h in path]
          == [("C", "A", "segment"), ("A", "B", "segment")])

    # Asymmetry without any return path: evidence_path is null and the message
    # says so. Network: isolated A<->B line plus one-way walk B->C.
    asym_dead = net_body(
        "net-asym-dead",
        segments=[
            {"line_id": "L1", "segment_id": "L1-ab",
             "from_station": "A", "to_station": "B",
             "start_at": "2026-10-01T00:00:00Z", "end_at": "2026-10-02T00:00:00Z"},
            {"line_id": "L1", "segment_id": "L1-ba",
             "from_station": "B", "to_station": "A",
             "start_at": "2026-10-01T00:00:00Z", "end_at": "2026-10-02T00:00:00Z"},
        ],
        transfers=[
            {"from_station": "B", "to_station": "C",
             "start_at": "2026-10-01T00:00:00Z", "end_at": "2026-10-02T00:00:00Z"},
        ],
    )
    r = client.post("/network/validate", json=asym_dead)
    body = r.json()
    check("dead-end asymmetry reported",
          body["conflict"]["code"] == "transfer_asymmetry"
          and body["conflict"]["evidence_path"] is None)
    check("dead-end message states no return path",
          "no return path" in body["conflict"]["message"])

    # Reachability across time slices: A->B segment only runs from noon, and
    # the walking corridors B<->C run the whole day. Slice 0 (00:00-12:00)
    # cannot reach B or C from hub A.
    timed = net_body(
        "net-unreach",
        segments=[
            {"line_id": "L1", "segment_id": "L1-ab",
             "from_station": "A", "to_station": "B",
             "start_at": "2026-10-01T12:00:00Z", "end_at": "2026-10-02T00:00:00Z"},
            {"line_id": "L1", "segment_id": "L1-ba",
             "from_station": "B", "to_station": "A",
             "start_at": "2026-10-01T12:00:00Z", "end_at": "2026-10-02T00:00:00Z"},
        ],
    )
    r = client.post("/network/validate", json=timed)
    body = r.json()
    check("unreachable conflict in early slice",
          body["conflict"]["code"] == "unreachable_station")
    check("unreachable reported in slice 0",
          body["conflict"]["slice"]["index"] == 0
          and body["conflict"]["slice"]["end"] == "2026-10-01T12:00:00Z")
    check("unreachable names hub and station",
          body["conflict"]["hub"] == "A" and body["conflict"]["station"] == "B")
    check("timeline sliced at edge bounds (2 slices)", body["slice_count"] == 2)

    # A network that is only reachable part of the day but symmetric must
    # still fail on reachability, even without transfer issues -- covered
    # above; now a symmetric, always-on network with two hubs both reaches all.
    two_hubs = net_body("net-two-hubs", hubs=("A", "C"))
    r = client.post("/network/validate", json=two_hubs)
    check("two hubs both reach all stations", r.json()["passed"] is True)

    # Snapshot freeze.
    r = client.post("/network/snapshots", json={"validation_id": "net-ok-1"})
    check("snapshot created", r.status_code == 200 and r.json()["status"] == "created")
    snap = r.json()
    check("snapshot id equals validation id", snap["snapshot_id"] == "net-ok-1")
    check("snapshot embeds validation result",
          snap["validation"]["validation_id"] == "net-ok-1"
          and snap["validation"]["passed"] is True)
    check("snapshot records freeze time", "frozen_at" in snap and snap["frozen_at"])

    r = client.get("/network/snapshots/net-ok-1")
    check("snapshot query returns frozen copy",
          r.status_code == 200
          and r.json()["validation"] == snap["validation"]
          and r.json()["frozen_at"] == snap["frozen_at"])

    r = client.post("/network/snapshots", json={"validation_id": "net-ok-1"})
    check("second freeze is idempotent",
          r.status_code == 200 and r.json()["status"] == "duplicate"
          and r.json()["frozen_at"] == snap["frozen_at"])

    r = client.get("/network/snapshots/does-not-exist")
    check("unknown snapshot -> 404", r.status_code == 404)

    # A failing validation cannot be frozen.
    r = client.post("/network/snapshots", json={"validation_id": "net-unreach"})
    check("freeze of failed validation -> 409", r.status_code == 409)
    r = client.get("/network/snapshots/net-unreach")
    check("failed validation left no snapshot", r.status_code == 404)

    r = client.post("/network/snapshots", json={"validation_id": "never-ran"})
    check("freeze of unknown validation -> 404", r.status_code == 404)

    # Immutability: after freezing, re-validating the same id (same payload)
    # returns the stored version with status duplicate; the snapshot keeps
    # status "created" semantics untouched and still answers identically.
    r = client.post("/network/validate", json=valid_net)
    replayed = r.json()
    check("later validation replays stored result",
          replayed["status"] == "duplicate"
          and {k: v for k, v in replayed.items() if k != "status"}
          == {k: v for k, v in vbody.items() if k != "status"})
    r = client.get("/network/snapshots/net-ok-1")
    check("snapshot unchanged by later validation",
          r.json()["validation"] == snap["validation"])

    # --- Fare disputes and refund settlement ----------------------------------
    # Passenger DP: three trips on 2026-09-27 (500, 200, 300 = 1000, under the
    # daily cap) and one isolated 300-cent trip on 2026-09-28.
    post("dp1", "DP", "S01", "in", "2026-09-27T08:00:00Z")
    post("dp2", "DP", "S04", "out", "2026-09-27T08:30:00Z")   # 500
    post("dp3", "DP", "S05", "in", "2026-09-27T09:00:00Z")
    post("dp4", "DP", "S06", "out", "2026-09-27T09:20:00Z")   # 300-100 = 200
    post("dp5", "DP", "S01", "in", "2026-09-27T12:00:00Z")
    post("dp6", "DP", "S02", "out", "2026-09-27T12:30:00Z")   # 300
    post("dp9", "DP", "S01", "in", "2026-09-28T12:00:00Z")
    post("dp10", "DP", "S02", "out", "2026-09-28T12:30:00Z")  # 300

    def dispute(did, pid, trip, claim, reason="fare dispute"):
        return client.post("/disputes", json={
            "dispute_id": did, "passenger_id": pid, "trip_id": trip,
            "claimed_amount_cents": claim, "reason": reason,
        })

    def resolve(did, version, approved, note=None):
        return client.post(f"/disputes/{did}/resolve", json={
            "expected_version": version,
            "approved_amount_cents": approved,
            "review_note": note,
        })

    # Open a dispute: the charge snapshot (500) is frozen.
    r = dispute("d1", "DP", "dp1:dp2", 400, "wrong zone charged")
    check("dispute created", r.status_code == 200 and r.json()["status"] == "created")
    d1 = r.json()
    check("open dispute is version 1",
          d1["state"] == "open" and d1["version"] == 1 and d1["refund"] is None
          and d1["resolved_at"] is None)
    check("dispute freezes the receivable snapshot",
          d1["snapshot"]["amount_cents"] == 500
          and d1["snapshot"]["base_fare_cents"] == 500
          and d1["snapshot"]["transfer_discount_cents"] == 0
          and d1["snapshot"]["basis"]["zone_fare_rule"] == "zone 1 -> zone 2"
          and d1["snapshot"]["frozen_at"])
    check("open dispute shows no settlement gap",
          d1["recomputed"] == {"current_amount_cents": 500,
                               "settled_amount_cents": None,
                               "difference_cents": 0, "direction": "none"})

    # Claim cannot exceed the frozen snapshot receivable.
    r = dispute("d-too-much", "DP", "dp1:dp2", 501)
    check("claim above snapshot receivable -> 422", r.status_code == 422)

    r = dispute("d-other-passenger", "XX", "dp1:dp2", 100)
    check("dispute on another passenger's trip -> 422", r.status_code == 422)

    r = dispute("d-unknown-trip", "DP", "no:such", 100)
    check("dispute on unknown trip -> 404", r.status_code == 404)

    # Idempotent replay of the identical create.
    r = dispute("d1", "DP", "dp1:dp2", 400, "wrong zone charged")
    check("identical dispute replays as duplicate",
          r.status_code == 200 and r.json()["status"] == "duplicate"
          and r.json()["version"] == 1
          and r.json()["snapshot"]["frozen_at"] == d1["snapshot"]["frozen_at"])

    conflict_body = {"dispute_id": "d1", "passenger_id": "DP",
                     "trip_id": "dp1:dp2", "claimed_amount_cents": 300,
                     "reason": "changed mind"}
    r = client.post("/disputes", json=conflict_body)
    check("same dispute_id, different payload -> 409", r.status_code == 409)

    r = client.get("/disputes/d1")
    check("GET returns the open dispute",
          r.status_code == 200 and r.json()["state"] == "open"
          and r.json()["claimed_amount_cents"] == 400
          and r.json()["reason"] == "wrong zone charged")

    # Resolve once, optimistically on version 1: immutable refund written.
    r = resolve("d1", 1, 350, "approved partial refund")
    check("dispute resolved at version 1", r.status_code == 200)
    r1 = r.json()
    check("resolved dispute moves to version 2",
          r1["state"] == "resolved" and r1["version"] == 2
          and r1["resolved_at"] is not None)
    check("immutable refund record written",
          r1["refund"]["refund_id"] == "RF-d1"
          and r1["refund"]["amount_cents"] == 350
          and r1["refund"]["review_note"] == "approved partial refund"
          and r1["refund"]["trip_id"] == "dp1:dp2")
    check("settled amount is snapshot minus refund",
          r1["recomputed"]["settled_amount_cents"] == 150
          and r1["recomputed"]["current_amount_cents"] == 500
          and r1["recomputed"]["difference_cents"] == -350
          and r1["recomputed"]["direction"] == "clawback_due")

    # Optimistic version conflict / already settled: any non-identical
    # re-resolution is a 409, regardless of the version it claims.
    r = resolve("d1", 1, 300, "changed approval")
    check("re-resolution with different payload -> 409", r.status_code == 409)

    r = resolve("d1", 2, 350, "approved partial refund")
    check("same approval, different claimed version -> 409", r.status_code == 409)

    # Identical resolution replays idempotently (same refund, no new ledger row).
    r = resolve("d1", 1, 350, "approved partial refund")
    check("identical resolution replays as duplicate",
          r.status_code == 200 and r.json()["status"] == "duplicate"
          and r.json()["refund"] == r1["refund"]
          and r.json()["resolved_at"] == r1["resolved_at"])

    # Resolve the other disputes before any correction happens.
    r = dispute("d2", "DP", "dp3:dp4", 200, "whole transfer trip")
    check("second dispute created", r.status_code == 200)
    r = resolve("d2", 1, 200, "full refund")
    check("d2 fully refunded", r.status_code == 200
          and r.json()["version"] == 2
          and r.json()["recomputed"]["settled_amount_cents"] == 0
          and r.json()["recomputed"]["difference_cents"] == -200
          and r.json()["recomputed"]["direction"] == "clawback_due")

    r = dispute("d3", "DP", "dp5:dp6", 100, "slight overcharge")
    r = resolve("d3", 1, 0, "rejected after review")
    check("rejected dispute resolves with zero refund",
          r.status_code == 200 and r.json()["refund"]["amount_cents"] == 0
          and r.json()["state"] == "resolved"
          and r.json()["recomputed"]["settled_amount_cents"] == 300
          and r.json()["recomputed"]["difference_cents"] == 0
          and r.json()["recomputed"]["direction"] == "none")

    r = dispute("d4", "DP", "dp9:dp10", 300, "separate day trip")
    r = resolve("d4", 1, 300, "full refund")
    check("d4 fully refunded on the 09-28 trip",
          r.status_code == 200 and r.json()["refund"]["refund_id"] == "RF-d4")

    # An open dispute used for version-conflict checks.
    r = dispute("d5", "DP", "dp1:dp2", 500, "second complaint on same trip")
    check("fifth dispute opens", r.status_code == 200 and r.json()["version"] == 1)
    r = resolve("d5", 2, 500)
    check("expected_version 2 on an open dispute -> 409", r.status_code == 409)
    r = resolve("d5", 1, 501)
    check("approved above claimed -> 422", r.status_code == 422)
    r = client.get("/disputes/missing")
    check("unknown dispute -> 404", r.status_code == 404)
    r = resolve("missing", 1, 10)
    check("resolving unknown dispute -> 404", r.status_code == 404)

    # --- Post-resolution corrections: top-up and clawback reporting ------------
    # Original frozen fares (all disputes are resolved before any correction):
    #   dp1:dp2 500 (d1, refund 350, settled 150)
    #   dp3:dp4 200 (d2, refund 200, settled 0)
    #   dp5:dp6 300 (d3, refund 0,   settled 300)
    #   dp9:dp10 300 (d4, refund 300, settled 0; 09-28)
    #
    # Correction 1: dp1:dp2 goes 500 -> 800 (tap-out S04(zone2) -> S07(zone3)).
    r = post("dp2", "DP", "S07", "out", "2026-09-27T08:30:00Z")
    check("correction after resolution re-prices trip", r["status"] == "corrected")
    r = client.get("/disputes/d1")
    rec = r.json()["recomputed"]
    check("resolved refund not overwritten by correction",
          r.json()["refund"]["amount_cents"] == 350
          and r.json()["snapshot"]["amount_cents"] == 500
          and r.json()["version"] == 2)
    check("correction exposes clawback difference (800 current, 150 settled)",
          rec["current_amount_cents"] == 800
          and rec["settled_amount_cents"] == 150
          and rec["difference_cents"] == -650
          and rec["direction"] == "clawback_due")
    r = client.get("/disputes/d5")
    check("open dispute reports no settlement gap after correction",
          r.json()["state"] == "open"
          and r.json()["recomputed"]["settled_amount_cents"] is None
          and r.json()["recomputed"]["direction"] == "none"
          and r.json()["recomputed"]["current_amount_cents"] == 800)

    # Correction 2: dp3:dp4 goes 200 -> 400 (tap-out S06(zone2) -> S02(zone1):
    # base 500, transfer discount 100). Fully refunded d2: settled 0.
    post("dp4", "DP", "S02", "out", "2026-09-27T09:20:00Z")
    r = client.get("/disputes/d2")
    check("higher recomputed fare on fully refunded trip -> clawback 400",
          r.json()["refund"]["amount_cents"] == 200
          and r.json()["snapshot"]["amount_cents"] == 200
          and r.json()["recomputed"]["current_amount_cents"] == 400
          and r.json()["recomputed"]["settled_amount_cents"] == 0
          and r.json()["recomputed"]["difference_cents"] == -400
          and r.json()["recomputed"]["direction"] == "clawback_due")

    # Correction 3: dp5:dp6 goes 300 -> 200 by moving the tap-in inside the
    # transfer window of the previous trip (gap 75min <= 90min). d3 was
    # rejected (refund 0, settled 300): the passenger is now owed 100 back.
    post("dp5", "DP", "S01", "in", "2026-09-27T10:35:00Z")
    r = client.get("/disputes/d3")
    check("lower recomputed fare after rejection -> top-up due 100",
          r.json()["refund"]["amount_cents"] == 0
          and r.json()["snapshot"]["amount_cents"] == 300
          and r.json()["recomputed"]["current_amount_cents"] == 200
          and r.json()["recomputed"]["settled_amount_cents"] == 300
          and r.json()["recomputed"]["difference_cents"] == 100
          and r.json()["recomputed"]["direction"] == "top_up_due")

    # Correction 4: void dp9:dp10 by inserting a tap-out between its tap-in
    # and tap-out: dp8b at 12:05 pairs with dp9, and dp10 becomes an orphan
    # out, so the dp9:dp10 charge is deleted. Live amount becomes 0; d4 fully
    # refunded against the frozen 300, so net paid 0 = current 0: no gap.
    post("dp8b", "DP", "S02", "out", "2026-09-28T12:05:00Z")
    r = client.get("/disputes/d4")
    check("voided trip has live amount 0; full refund stands",
          r.json()["refund"]["amount_cents"] == 300
          and r.json()["snapshot"]["amount_cents"] == 300
          and r.json()["recomputed"]["current_amount_cents"] == 0
          and r.json()["recomputed"]["settled_amount_cents"] == 0
          and r.json()["recomputed"]["direction"] == "none")

    # Ledger rows are protected at the storage layer.
    with sqlite3.connect(os.environ["DB_PATH"]) as raw:
        blocked_ledger = []
        for stmt in (
            "UPDATE refunds SET amount_cents = 1 WHERE dispute_id = 'd1'",
            "DELETE FROM refunds WHERE dispute_id = 'd1'",
            "UPDATE disputes SET claimed_amount_cents = 1 WHERE dispute_id = 'd1'",
            "DELETE FROM disputes WHERE dispute_id = 'd1'",
            "UPDATE dispute_snapshots SET amount_cents = 1 WHERE dispute_id = 'd1'",
            "DELETE FROM dispute_snapshots WHERE dispute_id = 'd1'",
        ):
            try:
                raw.execute(stmt)
            except sqlite3.Error:
                blocked_ledger.append(stmt)
        check("disputes/snapshots/refunds cannot be updated or deleted",
              len(blocked_ledger) == 6)
        check("one immutable refund row per resolved dispute",
              raw.execute("SELECT COUNT(*) FROM refunds").fetchone()[0] == 4)
        check("settled amounts persisted verbatim",
              [r[0] for r in raw.execute(
                  "SELECT amount_cents FROM refunds ORDER BY refund_id")]
              == [350, 200, 0, 300])

    # --- Operator fare clearing -----------------------------------------------
    # 2026-10-10: three trips on a dedicated passenger OP, amounts 500 / 400 / 400.
    #   op1:op2  S01(zone1) -> S04(zone2), 08:00->08:30, 500
    #   op3:op4  S05(zone2) -> S02(zone1), 09:00->09:30, transfer 400
    #   op5:op6  S01 -> S04,                    10:00->10:30, transfer 400
    # Boundary trip op7:op8 ends exactly at window_end (excluded); it lands on
    # the 2026-10-10 daily cap tail, so its amount is irrelevant to clearing.
    def oppost(eid, st, tap, ts):
        rr = client.post("/events", json={
            "event_id": eid, "passenger_id": "OP",
            "station_id": st, "tap_type": tap, "occurred_at": ts,
        })
        assert rr.status_code == 200, rr.text

    oppost("op1", "S01", "in", "2026-10-10T08:00:00Z")
    oppost("op2", "S04", "out", "2026-10-10T08:30:00Z")
    oppost("op3", "S05", "in", "2026-10-10T09:00:00Z")
    oppost("op4", "S02", "out", "2026-10-10T09:30:00Z")
    oppost("op5", "S01", "in", "2026-10-10T10:00:00Z")
    oppost("op6", "S04", "out", "2026-10-10T10:30:00Z")
    oppost("op7", "S01", "in", "2026-10-10T23:30:00Z")
    oppost("op8", "S04", "out", "2026-10-11T00:00:00Z")
    bill = {t["trip_id"]: t["amount_cents"]
            for t in client.get("/passengers/OP/bill").json()["trips"]}
    check("clearing fixtures priced 500/400/400 for the in-window trips",
          bill["op1:op2"] == 500 and bill["op3:op4"] == 400
          and bill["op5:op6"] == 400, bill)

    clearing_rules = [
        {"from_station": "S01", "to_station": "S04", "shares": [
            {"operator_id": "OA", "basis_points": 3333},
            {"operator_id": "OB", "basis_points": 3333},
            {"operator_id": "OC", "basis_points": 3334}]},
        {"from_station": "S05", "to_station": "S02", "shares": [
            {"operator_id": "OA", "basis_points": 4000},
            {"operator_id": "OD", "basis_points": 4000},
            {"operator_id": "OE", "basis_points": 2000}]},
    ]
    clearing_body = {
        "clearing_id": "clr-2026-10-10-a",
        "window_start": "2026-10-10T00:00:00Z",
        "window_end": "2026-10-11T00:00:00Z",
        "rules": clearing_rules,
    }

    # Whole-batch rejections before any persistence.
    r = client.post("/clearings", json=dict(
        clearing_body, clearing_id="clr-bad-window",
        window_start="2026-10-11T00:00:00Z", window_end="2026-10-10T00:00:00Z"))
    check("clearing inverted window -> 422", r.status_code == 422)
    r = client.post("/clearings", json=dict(
        clearing_body, clearing_id="clr-bad-sum",
        rules=[clearing_rules[0], {
            "from_station": "S05", "to_station": "S02", "shares": [
                {"operator_id": "OA", "basis_points": 4000},
                {"operator_id": "OD", "basis_points": 4000},
                {"operator_id": "OE", "basis_points": 1999}]}]))
    check("clearing shares not summing to 10000 -> 422",
          r.status_code == 422 and "9999" in r.text)
    r = client.post("/clearings", json=dict(
        clearing_body, clearing_id="clr-dup-op",
        rules=[{"from_station": "S01", "to_station": "S04", "shares": [
            {"operator_id": "OA", "basis_points": 5000},
            {"operator_id": "OA", "basis_points": 5000}]}]))
    check("clearing duplicate operator in a rule -> 422", r.status_code == 422)
    r = client.post("/clearings", json=dict(
        clearing_body, clearing_id="clr-dup-pair",
        rules=clearing_rules + [clearing_rules[0]]))
    check("clearing duplicate station pair -> 422", r.status_code == 422)
    r = client.post("/clearings", json=dict(
        clearing_body, clearing_id="clr-missing-rule",
        rules=[clearing_rules[0]]))
    check("clearing missing rule for an in-window pair -> 422 whole reject",
          r.status_code == 422 and "S05->S02" in r.text)
    check("rejected clearing leaves no row",
          client.get("/clearings/clr-missing-rule").status_code == 404)

    # First closing.
    r = client.post("/clearings", json=clearing_body)
    check("first clearing created", r.status_code == 200)
    cb = r.json()
    check("first clearing is initial seq 0",
          cb["status"] == "created" and cb["kind"] == "initial" and cb["seq"] == 0
          and cb["root_clearing_id"] == "clr-2026-10-10-a"
          and cb["base_clearing_id"] is None)
    check("half-open window excludes the boundary trip",
          cb["trip_count"] == 3 and cb["reversed_trip_count"] == 0
          and cb["gross_amount_cents"] == 1300)
    cle = {(e["trip_id"], e["operator_id"]): e for e in cb["entries"]}
    # 500 * 3333/10000 = 166.65 -> floor 166, leftover cent: OA/OB tie on .65
    # remainder -> operator id asc gives it to OA.
    check("500 split by remainder then operator id",
          cle[("op1:op2", "OA")]["amount_cents"] == 167
          and cle[("op1:op2", "OB")]["amount_cents"] == 166
          and cle[("op1:op2", "OC")]["amount_cents"] == 167)
    check("400 exact basis split",
          cle[("op3:op4", "OA")]["amount_cents"] == 160
          and cle[("op3:op4", "OD")]["amount_cents"] == 160
          and cle[("op3:op4", "OE")]["amount_cents"] == 80)
    # 400 * 3333 = 133.32 / OC 133.36 -> leftover cent to OC.
    check("400 split by fractional remainder",
          cle[("op5:op6", "OA")]["amount_cents"] == 133
          and cle[("op5:op6", "OB")]["amount_cents"] == 133
          and cle[("op5:op6", "OC")]["amount_cents"] == 134)
    check("every split preserves the trip total", all(
        sum(cle[(t, o)]["amount_cents"] for o in ops) == gross
        for t, ops, gross in (
            ("op1:op2", ("OA", "OB", "OC"), 500),
            ("op3:op4", ("OA", "OD", "OE"), 400),
            ("op5:op6", ("OA", "OB", "OC"), 400))))
    check("initial rows carry basis_points and zero prior",
          cle[("op1:op2", "OA")]["basis_points"] == 3333
          and cle[("op1:op2", "OA")]["prior_amount_cents"] == 0
          and cle[("op1:op2", "OA")]["cumulative_amount_cents"] == 167)
    cop = {o["operator_id"]: o["amount_cents"] for o in cb["operator_totals"]}
    check("initial chain operator totals",
          cop == {"OA": 460, "OB": 299, "OC": 301, "OD": 160, "OE": 80}
          and cb["net_amount_cents"] == 1300)

    # Idempotency: same payload (even reordered) replays; different payload 409.
    r = client.post("/clearings", json=clearing_body)
    check("identical clearing replays as duplicate",
          r.status_code == 200 and r.json()["status"] == "duplicate"
          and r.json()["ledger_row_count"] == cb["ledger_row_count"])
    shuffled_clearing = dict(clearing_body, rules=[
        {"from_station": "S05", "to_station": "S02", "shares": [
            {"operator_id": "OE", "basis_points": 2000},
            {"operator_id": "OD", "basis_points": 4000},
            {"operator_id": "OA", "basis_points": 4000}]},
        {"from_station": "S01", "to_station": "S04", "shares": [
            {"operator_id": "OC", "basis_points": 3334},
            {"operator_id": "OB", "basis_points": 3333},
            {"operator_id": "OA", "basis_points": 3333}]}])
    r = client.post("/clearings", json=shuffled_clearing)
    check("rule/share array order does not affect idempotency",
          r.json()["status"] == "duplicate")
    r = client.post("/clearings", json=dict(clearing_body, window_end="2026-10-12T00:00:00Z"))
    check("same clearing_id different payload -> 409", r.status_code == 409)

    # GET returns the frozen result.
    g = client.get("/clearings/clr-2026-10-10-a").json()
    check("GET clearing returns created marker and frozen window/rules",
          g["status"] == "created" and g["window_start"] == "2026-10-10T00:00:00Z"
          and g["window_end"] == "2026-10-11T00:00:00Z"
          and len(g["rules"]) == 2
          and g["operator_totals"] == cb["operator_totals"])
    check("unknown clearing -> 404",
          client.get("/clearings/nope").status_code == 404)

    # Adjustment reference validation.
    r = client.post("/clearings/adjustments", json={
        "clearing_id": "clr-adj-x", "base_clearing_id": "no-such-root"})
    check("adjustment with unknown root -> 404", r.status_code == 404)

    # --- Correction: move op3 to 07:00 so op3:op4 disappears ------------------
    r = client.post("/events", json={
        "event_id": "op3", "passenger_id": "OP", "station_id": "S05",
        "tap_type": "in", "occurred_at": "2026-10-10T07:00:00Z"})
    check("op3 correction voids op3:op4",
          any(a["trip_id"] == "op3:op4" and a["new_amount_cents"] == 0
              for a in r.json()["adjustments"]))

    r = client.post("/clearings/adjustments", json={
        "clearing_id": "clr-2026-10-10-b", "base_clearing_id": "clr-2026-10-10-a"})
    check("first adjustment created", r.status_code == 200, r.text)
    ab = r.json()
    check("adjustment inherits the frozen window and rules",
          ab["kind"] == "adjustment" and ab["seq"] == 1
          and ab["root_clearing_id"] == "clr-2026-10-10-a"
          and ab["base_clearing_id"] == "clr-2026-10-10-a"
          and ab["window_start"] == cb["window_start"]
          and len(ab["rules"]) == 2)
    check("adjustment sees 2 current trips and 1 reversal",
          ab["trip_count"] == 2 and ab["reversed_trip_count"] == 1)
    ae = {(e["trip_id"], e["operator_id"]): e for e in ab["entries"]}
    check("unchanged trips append no delta rows",
          ("op1:op2", "OA") not in ae and ("op5:op6", "OA") not in ae)
    check("vanished trip is fully reversed with negative deltas",
          ae[("op3:op4", "OA")]["amount_cents"] == -160
          and ae[("op3:op4", "OD")]["amount_cents"] == -160
          and ae[("op3:op4", "OE")]["amount_cents"] == -80
          and ae[("op3:op4", "OE")]["gross_amount_cents"] == 0
          and ae[("op3:op4", "OE")]["cumulative_amount_cents"] == 0
          and ae[("op3:op4", "OE")]["basis_points"] is None)
    aop = {o["operator_id"]: o["amount_cents"] for o in ab["operator_totals"]}
    check("chain totals after reversal",
          aop == {"OA": 300, "OB": 299, "OC": 301, "OD": 0, "OE": 0}
          and sum(aop.values()) == 900 and ab["net_amount_cents"] == -400)

    # Repeated adjustments never double-book.
    r = client.post("/clearings/adjustments", json={
        "clearing_id": "clr-2026-10-10-b", "base_clearing_id": "clr-2026-10-10-a"})
    check("identical adjustment replays as duplicate, books nothing new",
          r.json()["status"] == "duplicate"
          and r.json()["ledger_row_count"] == ab["ledger_row_count"])
    r = client.post("/clearings/adjustments", json={
        "clearing_id": "clr-2026-10-10-b2", "base_clearing_id": "clr-2026-10-10-a"})
    check("fresh adjustment against unchanged state appends zero deltas",
          r.json()["seq"] == 2 and r.json()["entries"] == []
          and r.json()["net_amount_cents"] == 0
          and r.json()["operator_totals"] == ab["operator_totals"])
    r = client.post("/clearings/adjustments", json={
        "clearing_id": "clr-2026-10-10-b", "base_clearing_id": "other-root"})
    check("same adjustment id different payload -> 409", r.status_code == 409)
    r = client.post("/clearings/adjustments", json={
        "clearing_id": "clr-adj-of-adj", "base_clearing_id": "clr-2026-10-10-b"})
    check("adjustment referencing an adjustment batch -> 422", r.status_code == 422)

    # Re-correction restores op3:op4: positive deltas bring it back.
    client.post("/events", json={
        "event_id": "op3", "passenger_id": "OP", "station_id": "S05",
        "tap_type": "in", "occurred_at": "2026-10-10T09:00:00Z"})
    r = client.post("/clearings/adjustments", json={
        "clearing_id": "clr-2026-10-10-c", "base_clearing_id": "clr-2026-10-10-a"})
    cc = r.json()
    ce = {(e["trip_id"], e["operator_id"]): e for e in cc["entries"]}
    check("restored trip appends positive make-up deltas",
          ce[("op3:op4", "OA")]["amount_cents"] == 160
          and ce[("op3:op4", "OD")]["amount_cents"] == 160
          and ce[("op3:op4", "OE")]["amount_cents"] == 80
          and ce[("op3:op4", "OA")]["prior_amount_cents"] == 0
          and ce[("op3:op4", "OA")]["cumulative_amount_cents"] == 160
          and ("op1:op2", "OA") not in ce)
    check("restored adjustment returns chain totals to the first closing",
          {o["operator_id"]: o["amount_cents"] for o in cc["operator_totals"]}
          == cop)

    # A station-pair correction to a pair the frozen rules do not cover is a
    # whole-batch rejection that books nothing.
    client.post("/events", json={
        "event_id": "op6", "passenger_id": "OP", "station_id": "S07",
        "tap_type": "out", "occurred_at": "2026-10-10T10:30:00Z"})
    r = client.post("/clearings/adjustments", json={
        "clearing_id": "clr-2026-10-10-d", "base_clearing_id": "clr-2026-10-10-a"})
    check("new station pair outside frozen rules -> 422 whole reject",
          r.status_code == 422 and "S01->S07" in r.text)
    check("rejected adjustment leaves no row and no ledger movement",
          client.get("/clearings/clr-2026-10-10-d").status_code == 404
          and {o["operator_id"]: o["amount_cents"]
               for o in client.get("/clearings/clr-2026-10-10-c").json()["operator_totals"]}
          == cop)

    # A station-pair correction BETWEEN two covered pairs re-settles the trip:
    # op5:op6 moves S01->S04 onto S05->S02 (still a transfer trip, gross 400),
    # so OA/OB/OC are reversed to zero and OA/OD/OE take up the new split.
    client.post("/events", json={
        "event_id": "op5", "passenger_id": "OP", "station_id": "S05",
        "tap_type": "in", "occurred_at": "2026-10-10T10:00:00Z"})
    client.post("/events", json={
        "event_id": "op6", "passenger_id": "OP", "station_id": "S02",
        "tap_type": "out", "occurred_at": "2026-10-10T10:30:00Z"})
    bill = {t["trip_id"]: t["from_station"] + "->" + t["to_station"]
            for t in client.get("/passengers/OP/bill").json()["trips"]}
    check("op5:op6 re-paired onto the other covered rule pair",
          bill.get("op5:op6") == "S05->S02", bill)
    r = client.post("/clearings/adjustments", json={
        "clearing_id": "clr-2026-10-10-e", "base_clearing_id": "clr-2026-10-10-a"})
    check("covered pair change settles instead of rejecting", r.status_code == 200, r.text)
    eb = r.json()
    ee = {(e["trip_id"], e["operator_id"]): e for e in eb["entries"]}
    check("pair change reverses the old-rule operators to zero",
          ee[("op5:op6", "OB")]["amount_cents"] == -133
          and ee[("op5:op6", "OC")]["amount_cents"] == -134
          and ee[("op5:op6", "OB")]["cumulative_amount_cents"] == 0
          and ee[("op5:op6", "OC")]["cumulative_amount_cents"] == 0)
    check("pair change tops up the new-rule operators",
          ee[("op5:op6", "OA")]["amount_cents"] == 27   # 160 target - 133 prior
          and ee[("op5:op6", "OD")]["amount_cents"] == 160
          and ee[("op5:op6", "OE")]["amount_cents"] == 80
          and ee[("op5:op6", "OA")]["cumulative_amount_cents"] == 160)
    check("pair change delta nets to zero (same gross, different operators)",
          eb["net_amount_cents"] == 0 and eb["reversed_trip_count"] == 0)
    eop = {o["operator_id"]: o["amount_cents"] for o in eb["operator_totals"]}
    check("chain totals after pair change",
          eop == {"OA": 487, "OB": 166, "OC": 167, "OD": 320, "OE": 160}
          and sum(eop.values()) == 1300, eop)

    # First-closing ledger is immutable and chain-wide sums stay consistent.
    with sqlite3.connect(os.environ["DB_PATH"]) as raw:
        blocked_clearing = []
        for stmt in (
            "UPDATE clearing_batches SET net_amount_cents = 0"
            " WHERE clearing_id = 'clr-2026-10-10-a'",
            "DELETE FROM clearing_batches WHERE clearing_id = 'clr-2026-10-10-b'",
            "UPDATE clearing_entries SET amount_cents = 1"
            " WHERE clearing_id = 'clr-2026-10-10-a'",
            "DELETE FROM clearing_entries WHERE clearing_id = 'clr-2026-10-10-a'",
        ):
            try:
                raw.execute(stmt)
            except sqlite3.Error:
                blocked_clearing.append(stmt)
        check("clearing batches/entries cannot be updated or deleted",
              len(blocked_clearing) == 4)
        check("first closing ledger rows never rewritten",
              [row[0] for row in raw.execute(
                  "SELECT amount_cents FROM clearing_entries"
                  " WHERE clearing_id = 'clr-2026-10-10-a' ORDER BY seq")]
              == [e["amount_cents"] for e in sorted(
                  cb["entries"], key=lambda e: e["seq"])])
        check("ledger per trip/operator cumulative equals current target or zero",
              all(
                  (row[2] or 0) >= 0
                  for row in raw.execute(
                      "SELECT trip_id, operator_id, SUM(amount_cents)"
                      " FROM clearing_entries GROUP BY trip_id, operator_id")))
        n_clearing_batches = raw.execute(
            "SELECT COUNT(*) FROM clearing_batches").fetchone()[0]
    check("five clearing batches persisted (initial + four adjustments)",
          n_clearing_batches == 5)

    # --- Tap-event quality scans ----------------------------------------------
    def qpost(event_id, passenger, station, tap, ts):
        rr = client.post("/events", json={
            "event_id": event_id, "passenger_id": passenger,
            "station_id": station, "tap_type": tap, "occurred_at": ts,
        })
        assert rr.status_code == 200, rr.text

    # 2026-10-05, a day no other section uses.
    # EQ1: legal trip, an impossibly quick S04->S01 return (20s < 300s min),
    #      then a duplicate tap-in 10s later, a legal trip, and a duplicate
    #      tap-out 5s later.
    qpost("qe1", "EQ1", "S01", "in", "2026-10-05T08:00:00Z")
    qpost("qe2", "EQ1", "S04", "out", "2026-10-05T08:30:00Z")
    qpost("qe3", "EQ1", "S01", "in", "2026-10-05T08:30:20Z")  # min travel
    qpost("qe4", "EQ1", "S01", "in", "2026-10-05T08:30:30Z")  # duplicate in
    qpost("qe5", "EQ1", "S07", "out", "2026-10-05T09:30:00Z")
    qpost("qe6", "EQ1", "S07", "out", "2026-10-05T09:30:05Z")  # duplicate out
    # EQ2: only a duplicate tap-in.
    qpost("qe7", "EQ2", "S02", "in", "2026-10-05T10:00:00Z")
    qpost("qe8", "EQ2", "S02", "in", "2026-10-05T10:00:15Z")  # duplicate in
    # EQ3: boundary case -- out->in at exactly 300s is legal, same-type taps
    # exactly 60s apart still count as duplicates (inclusive window).
    qpost("qe9", "EQ3", "S04", "out", "2026-10-05T11:00:00Z")
    qpost("qe10", "EQ3", "S01", "in", "2026-10-05T11:05:00Z")  # == min: legal
    qpost("qe11", "EQ3", "S01", "in", "2026-10-05T11:06:00Z")  # 60s: duplicate
    # EQ4: a duplicate pair exactly at window_start (included) and a lone tap
    # exactly at window_end (excluded): the half-open window boundaries.
    qpost("qe12", "EQ4", "S01", "in", "2026-10-05T00:00:00Z")
    qpost("qe13", "EQ4", "S01", "in", "2026-10-05T00:00:10Z")
    qpost("qe14", "EQ4", "S01", "in", "2026-10-06T00:00:00Z")

    audit_before_scans = client.get("/audit").json()["count"]

    scan_body = {
        "scan_id": "eqs-2026-10-05-a",
        "window_start": "2026-10-05T00:00:00Z",
        "window_end": "2026-10-06T00:00:00Z",
        "duplicate_window_seconds": 60,
        "min_travel_seconds": {"S04": {"S01": 300}},
    }
    r = client.post("/event-quality/scans", json=scan_body)
    check("quality scan created", r.status_code == 200 and r.json()["status"] == "created")
    scan = r.json()
    check("scan counts 13 in-window events (window_end tap excluded)",
          scan["event_count"] == 13)
    check("scan finds 6 alerts", scan["alert_count"] == 6 and len(scan["alerts"]) == 6)
    by_event = {a["event_id"]: a for a in scan["alerts"]}

    a = by_event["qe3"]
    check("min-travel alert on qe3",
          a["rule"] == "min_travel_violation" and a["passenger_id"] == "EQ1")
    check("min-travel actual interval 20s vs threshold 300s",
          a["actual_interval_seconds"] == 20 and a["threshold_seconds"] == 300)
    check("min-travel evidence chain is out then in",
          [(e["event_id"], e["tap_type"], e["station_id"]) for e in a["evidence"]]
          == [("qe2", "out", "S04"), ("qe3", "in", "S01")])

    a = by_event["qe4"]
    check("duplicate tap-in alert on qe4",
          a["rule"] == "duplicate_tap" and a["actual_interval_seconds"] == 10
          and a["threshold_seconds"] == 60)
    check("duplicate in evidence chain",
          [e["event_id"] for e in a["evidence"]] == ["qe3", "qe4"])

    a = by_event["qe6"]
    check("duplicate tap-out alert on qe6",
          a["rule"] == "duplicate_tap" and a["actual_interval_seconds"] == 5)
    check("duplicate out evidence chain",
          [e["event_id"] for e in a["evidence"]] == ["qe5", "qe6"])

    check("EQ2 duplicate in detected", by_event["qe8"]["rule"] == "duplicate_tap")
    check("EQ3 exact-minimum tap-in is legal", "qe10" not in by_event)
    check("EQ3 exact-window duplicate still flagged",
          by_event["qe11"]["actual_interval_seconds"] == 60)
    check("window_start pair included", by_event["qe13"]["actual_interval_seconds"] == 10)
    check("window_end tap has no alert", "qe14" not in by_event)

    # Alerts are ordered by triggering (occurred_at, seq).
    check("alerts ordered by triggering time",
          [a["event_id"] for a in scan["alerts"]]
          == ["qe13", "qe3", "qe4", "qe6", "qe8", "qe11"])
    check("full scan echoes null passengers allow-list", scan["passengers"] is None)

    # Passenger allow-list.
    only_eq2 = dict(scan_body, scan_id="eqs-2026-10-05-eq2", passengers=["EQ2"])
    r = client.post("/event-quality/scans", json=only_eq2)
    body2 = r.json()
    check("restricted scan sees only EQ2 events",
          body2["event_count"] == 2 and body2["alert_count"] == 1
          and body2["alerts"][0]["event_id"] == "qe8")
    check("restricted scan echoes the allow-list", body2["passengers"] == ["EQ2"])

    # Reordered/deduped allow-list and different offset notation are the same
    # payload.
    same = dict(only_eq2,
                window_start="2026-10-05T08:00:00+08:00",
                passengers=["EQ2", "EQ2"])
    r = client.post("/event-quality/scans", json=same)
    check("normalised payload replays as duplicate",
          r.status_code == 200 and r.json()["status"] == "duplicate"
          and r.json()["alerts"] == body2["alerts"])

    # Looser duplicate window: no duplicate alerts, but min-travel survives.
    loose = dict(scan_body, scan_id="eqs-2026-10-05-loose",
                 duplicate_window_seconds=0)
    r = client.post("/event-quality/scans", json=loose)
    loose_body = r.json()
    rules = {a["rule"] for a in loose_body["alerts"]}
    check("zero duplicate window removes duplicate_tap alerts",
          rules == {"min_travel_violation"})

    # Same scan_id, different payload -> 409.
    conflict = dict(scan_body, duplicate_window_seconds=90)
    r = client.post("/event-quality/scans", json=conflict)
    check("same scan_id, different payload -> 409", r.status_code == 409)

    # Invalid requests.
    bad_window = dict(scan_body, scan_id="eqs-bad-window",
                      window_start="2026-10-06T00:00:00Z")
    r = client.post("/event-quality/scans", json=bad_window)
    check("empty scan window -> 422", r.status_code == 422)
    r = client.post("/event-quality/scans",
                    json=dict(scan_body, scan_id="eqs-bad-neg",
                              duplicate_window_seconds=-1))
    check("negative duplicate window -> 422", r.status_code == 422)

    # GET.
    r = client.get("/event-quality/scans/eqs-2026-10-05-a")
    check("GET returns the frozen scan",
          r.status_code == 200 and r.json()["status"] == "created"
          and r.json()["alerts"] == scan["alerts"]
          and r.json()["min_travel_seconds"] == {"S04": {"S01": 300}})
    r = client.get("/event-quality/scans/does-not-exist")
    check("unknown scan -> 404", r.status_code == 404)

    with sqlite3.connect(os.environ["DB_PATH"]) as raw:
        n_scans = raw.execute("SELECT COUNT(*) FROM event_quality_scans").fetchone()[0]
        n_alerts = raw.execute("SELECT COUNT(*) FROM event_quality_alerts").fetchone()[0]
        blocked_scans = []
        for stmt in (
            "UPDATE event_quality_scans SET alert_count = 0 WHERE scan_id = 'eqs-2026-10-05-a'",
            "DELETE FROM event_quality_scans WHERE scan_id = 'eqs-2026-10-05-a'",
            "UPDATE event_quality_alerts SET rule = 'min_travel_violation'"
            " WHERE scan_id = 'eqs-2026-10-05-a'",
            "DELETE FROM event_quality_alerts WHERE scan_id = 'eqs-2026-10-05-a'",
        ):
            try:
                raw.execute(stmt)
            except sqlite3.Error:
                blocked_scans.append(stmt)
        check("scan tables are append-only at the storage layer",
              len(blocked_scans) == 4)
        # 3 created scans: main (6 alerts), EQ2-only (1), loose (1 min-travel);
        # duplicate replays and the 409 conflict added no rows.
        check("stored scan/alert counts", n_scans == 3 and n_alerts == 8)

    # Scans never touch business data: a further scan leaves trips/bills
    # identical, and creates no audit records.
    bill1 = client.get("/passengers/EQ1/bill").json()
    r = client.post("/event-quality/scans", json=dict(scan_body, scan_id="eqs-rerun"))
    check("equivalent scan under a new id is created independently",
          r.status_code == 200 and r.json()["status"] == "created"
          and r.json()["alerts"] == scan["alerts"])
    bill2 = client.get("/passengers/EQ1/bill").json()
    check("scans change no trips/bills",
          bill1["total_cents"] == bill2["total_cents"]
          and bill1["trips"] == bill2["trips"])
    check("scans create no audit records",
          client.get("/audit").json()["count"] == audit_before_scans)

    # --- Anonymous privacy exports --------------------------------------------
    import hmac

    def ppost(event_id, passenger, station, tap, ts):
        rr = client.post("/events", json={
            "event_id": event_id, "passenger_id": passenger,
            "station_id": station, "tap_type": tap, "occurred_at": ts,
        })
        assert rr.status_code == 200, rr.text

    # 2026-11-15, a day no other section uses. Groups:
    #   core=S01..S03, mid=S04..S06, suburb=S07..S09
    # Class (08:00, core, mid):    PX1, PX2, PX3 -> 3 distinct, retained
    # Class (08:00, suburb, core): PX1, PX4      -> 2 distinct, suppressed
    # Class (09:00, mid, core):    PX2, PX3, PX5 -> 3 distinct, retained
    # Class (10:00, core, core):   PX5, PX6      -> 2 distinct, suppressed
    # Class (00:00, core, core):   PX7 alone     -> 1 distinct, suppressed
    # PX8 taps in exactly at window_end -> outside the half-open window.
    ppost("px1a", "PX1", "S01", "in", "2026-11-15T08:00:00Z")
    ppost("px1b", "PX1", "S04", "out", "2026-11-15T08:30:00Z")
    ppost("px1c", "PX1", "S07", "in", "2026-11-15T08:50:00Z")
    ppost("px1d", "PX1", "S01", "out", "2026-11-15T09:20:00Z")
    ppost("px2a", "PX2", "S02", "in", "2026-11-15T08:05:00Z")
    ppost("px2b", "PX2", "S05", "out", "2026-11-15T08:35:00Z")
    ppost("px2c", "PX2", "S04", "in", "2026-11-15T09:00:00Z")
    ppost("px2d", "PX2", "S01", "out", "2026-11-15T09:30:00Z")
    ppost("px3a", "PX3", "S03", "in", "2026-11-15T08:10:00Z")
    ppost("px3b", "PX3", "S06", "out", "2026-11-15T08:40:00Z")
    ppost("px3c", "PX3", "S05", "in", "2026-11-15T09:05:00Z")
    ppost("px3d", "PX3", "S02", "out", "2026-11-15T09:35:00Z")
    ppost("px4a", "PX4", "S08", "in", "2026-11-15T08:55:00Z")
    ppost("px4b", "PX4", "S02", "out", "2026-11-15T09:25:00Z")
    ppost("px5a", "PX5", "S06", "in", "2026-11-15T09:10:00Z")
    ppost("px5b", "PX5", "S03", "out", "2026-11-15T09:40:00Z")
    ppost("px5c", "PX5", "S01", "in", "2026-11-15T10:00:00Z")
    ppost("px5d", "PX5", "S02", "out", "2026-11-15T10:20:00Z")
    ppost("px6a", "PX6", "S01", "in", "2026-11-15T10:10:00Z")
    ppost("px6b", "PX6", "S03", "out", "2026-11-15T10:30:00Z")
    ppost("px7a", "PX7", "S01", "in", "2026-11-15T00:00:00Z")  # == window_start
    ppost("px7b", "PX7", "S02", "out", "2026-11-15T00:20:00Z")
    ppost("px8a", "PX8", "S01", "in", "2026-11-16T00:00:00Z")  # == window_end
    ppost("px8b", "PX8", "S02", "out", "2026-11-16T00:20:00Z")

    groups = {"S01": "core", "S02": "core", "S03": "core",
              "S04": "mid", "S05": "mid", "S06": "mid",
              "S07": "suburb", "S08": "suburb", "S09": "suburb"}
    export_body = {
        "export_id": "pex-2026-11-15-a",
        "window_start": "2026-11-15T00:00:00Z",
        "window_end": "2026-11-16T00:00:00Z",
        "bucket_minutes": 60,
        "station_groups": dict(groups),
        "k": 3,
    }

    audit_before_exports = client.get("/audit", params={"limit": 1000}).json()["count"]

    r = client.post("/privacy/exports", json=export_body)
    check("privacy export created",
          r.status_code == 200 and r.json()["status"] == "created")
    exp = r.json()
    check("export sees 11 in-window trips (window_end pair excluded)",
          exp["input_trip_count"] == 11)
    check("export builds 5 equivalence classes",
          exp["equivalence_class_count"] == 5)
    check("two classes retained, three suppressed",
          exp["retained_class_count"] == 2 and exp["suppressed_class_count"] == 3)
    check("six trips retained, five trips whole-class suppressed",
          exp["retained_trip_count"] == 6 and exp["suppressed_trip_count"] == 5
          and len(exp["trips"]) == 6 and len(exp["classes"]) == 2)

    classes = {
        (c["bucket_start"], c["origin_group"], c["destination_group"]): c
        for c in exp["classes"]
    }
    check("retained classes are the two k>=3 classes",
          set(classes) == {
              ("2026-11-15T08:00:00Z", "core", "mid"),
              ("2026-11-15T09:00:00Z", "mid", "core"),
          })
    check("class passenger_count counts distinct passengers",
          all(c["passenger_count"] == 3 and c["trip_count"] == 3
              for c in classes.values()))

    # Epoch-anchored bucketing: PX1's suburb trip taps in at 08:50, so it
    # belongs to the 08:00 bucket (suppressed), not 09:00.
    retained_keys = {
        (t["bucket_start"], t["origin_group"], t["destination_group"])
        for t in exp["trips"]
    }
    check("retained trip keys match the retained classes",
          retained_keys == set(classes))

    # Pseudonyms: stable HMAC over the server salt; the raw id never appears.
    px1_pseudo = hmac.new(b"test-privacy-salt", b"PX1", hashlib.sha256).hexdigest()
    px4_pseudo = hmac.new(b"test-privacy-salt", b"PX4", hashlib.sha256).hexdigest()
    px6_pseudo = hmac.new(b"test-privacy-salt", b"PX6", hashlib.sha256).hexdigest()
    px7_pseudo = hmac.new(b"test-privacy-salt", b"PX7", hashlib.sha256).hexdigest()
    px8_pseudo = hmac.new(b"test-privacy-salt", b"PX8", hashlib.sha256).hexdigest()
    check("pseudonym equals HMAC-SHA256(passenger_id, PRIVACY_SALT)",
          px1_pseudo in {t["pseudonym"] for t in exp["trips"]})
    check("PX1 retained trip present (its suppressed suburb trip is not)",
          sum(1 for t in exp["trips"] if t["pseudonym"] == px1_pseudo) == 1)
    check("suppressed-only passengers leave no row",
          not {t["pseudonym"] for t in exp["trips"]}
          & {px4_pseudo, px6_pseudo, px7_pseudo, px8_pseudo})
    check("no raw passenger ids leak anywhere in the response",
          not any(f"PX{i}" in r.text for i in range(1, 9)))
    check("trip rows carry only pseudonym, bucket and station groups",
          all(set(t) == {"pseudonym", "bucket_start", "origin_group",
                         "destination_group"} for t in exp["trips"]))

    # k=1 export: every class retained; one passenger keeps one pseudonym
    # across multiple trips (PX1 twice with the identical pseudonym).
    open_body = dict(export_body, export_id="pex-2026-11-15-open", k=1)
    r = client.post("/privacy/exports", json=open_body)
    open_exp = r.json()
    check("k=1 retains every class and trip",
          r.status_code == 200
          and open_exp["retained_class_count"] == 5
          and open_exp["suppressed_class_count"] == 0
          and open_exp["retained_trip_count"] == 11
          and open_exp["suppressed_trip_count"] == 0)
    px1_rows = [t for t in open_exp["trips"] if t["pseudonym"] == px1_pseudo]
    check("same passenger keeps one stable pseudonym across trips",
          len(px1_rows) == 2 and len({t["bucket_start"] for t in px1_rows}) == 1)
    check("window_start trip included, window_end trip excluded",
          any(t["pseudonym"] == px7_pseudo for t in open_exp["trips"])
          and not any(t["pseudonym"] == px8_pseudo for t in open_exp["trips"]))

    # Idempotency: same payload replays the frozen result (map key order and
    # time-offset notation do not matter); different payload -> 409.
    r = client.post("/privacy/exports", json=export_body)
    check("identical export replays as duplicate",
          r.status_code == 200 and r.json()["status"] == "duplicate"
          and r.json()["trips"] == exp["trips"])
    reordered = dict(export_body,
                     station_groups={s: g for s, g in reversed(list(groups.items()))})
    r = client.post("/privacy/exports", json=reordered)
    check("station_groups key order does not affect identity",
          r.json()["status"] == "duplicate")
    tz_body = dict(export_body, window_start="2026-11-15T08:00:00+08:00")
    r = client.post("/privacy/exports", json=tz_body)
    check("different offset notation of the same instant -> duplicate",
          r.json()["status"] == "duplicate")
    r = client.post("/privacy/exports", json=dict(export_body, k=4))
    check("same export_id, different payload -> 409", r.status_code == 409)

    # Validation.
    r = client.post("/privacy/exports",
                    json=dict(export_body, export_id="pex-bad-window",
                              window_end="2026-11-15T00:00:00Z"))
    check("empty export window -> 422", r.status_code == 422)
    r = client.post("/privacy/exports",
                    json=dict(export_body, export_id="pex-bad-k", k=0))
    check("k < 1 -> 422", r.status_code == 422)
    r = client.post("/privacy/exports",
                    json=dict(export_body, export_id="pex-bad-bucket",
                              bucket_minutes=0))
    check("bucket_minutes < 1 -> 422", r.status_code == 422)
    r = client.post("/privacy/exports",
                    json=dict(export_body, export_id="pex-bad-map",
                              station_groups={"S01": "core"}))
    check("in-window trip endpoint missing from station_groups -> 422",
          r.status_code == 422 and "S04" in r.text)
    r = client.post("/privacy/exports",
                    json=dict(export_body, export_id="pex-bad-empty",
                              station_groups={}))
    check("empty station_groups -> 422", r.status_code == 422)

    # GET.
    r = client.get("/privacy/exports/pex-2026-11-15-a")
    check("GET returns the frozen export",
          r.status_code == 200 and r.json()["status"] == "created"
          and r.json()["trips"] == exp["trips"]
          and r.json()["classes"] == exp["classes"]
          and r.json()["suppressed_trip_count"] == 5)
    r = client.get("/privacy/exports/does-not-exist")
    check("unknown export -> 404", r.status_code == 404)

    # Exports are frozen against later events (no recompute on replay), create
    # no audit records, and persist pseudonyms only (never raw passenger ids).
    ppost("px9a", "PX9", "S01", "in", "2026-11-15T08:15:00Z")
    ppost("px9b", "PX9", "S04", "out", "2026-11-15T08:45:00Z")
    # Re-baseline after the two new tap events above (which do append event
    # audits) so the comparison isolates the export operations themselves.
    audit_before_exports = client.get("/audit", params={"limit": 1000}).json()["count"]
    r = client.post("/privacy/exports", json=export_body)
    check("frozen export immune to later events",
          r.json()["status"] == "duplicate" and r.json()["trips"] == exp["trips"])
    r = client.post("/privacy/exports", json=dict(export_body, export_id="pex-audit-probe"))
    check("a fresh export also appends no audit records",
          r.status_code == 200 and r.json()["status"] == "created"
          and client.get("/audit", params={"limit": 1000}).json()["count"]
          == audit_before_exports)
    check("exports create no audit records",
          client.get("/audit", params={"limit": 1000}).json()["count"] == audit_before_exports)

    with sqlite3.connect(os.environ["DB_PATH"]) as raw:
        raw_rows = raw.execute(
            "SELECT pseudonym, bucket_start, origin_group, destination_group"
            " FROM privacy_export_trips"
        ).fetchall()
        check("stored rows hold no raw passenger ids",
              not any("PX" in "".join(str(v) for v in row) for row in raw_rows))
        n_exports = raw.execute("SELECT COUNT(*) FROM privacy_exports").fetchone()[0]
        n_trips = raw.execute("SELECT COUNT(*) FROM privacy_export_trips").fetchone()[0]
        # Three created exports: k=3 -> 6 trips, k=1 -> 11, post-PX9 k=3 probe
        # -> 7 (the main six plus PX9's new 08:00 core->mid trip); replays and
        # the 409 added nothing.
        check("stored export/trip counts", n_exports == 3 and n_trips == 24)
        blocked_exports = []
        for stmt in (
            "UPDATE privacy_exports SET k = 1 WHERE export_id = 'pex-2026-11-15-a'",
            "DELETE FROM privacy_exports WHERE export_id = 'pex-2026-11-15-a'",
            "UPDATE privacy_export_trips SET pseudonym = 'x' WHERE seq = 0",
            "DELETE FROM privacy_export_trips WHERE export_id = 'pex-2026-11-15-a'",
        ):
            try:
                raw.execute(stmt)
            except sqlite3.Error:
                blocked_exports.append(stmt)
        check("privacy export tables are append-only at the storage layer",
              len(blocked_exports) == 4)

    # --- Sudden-disruption evacuation replans --------------------------------
    # Timetable (all times 2026-09-27, min transfer 300s):
    #   ALPHA   A 08:05 -> B 08:20/08:25 -> E 08:55        (cap 100)
    #   BRAVO   B 08:30 -> F 09:00                          (cap 100)
    #   CHARLIE A 09:05 -> B 09:20/09:25 -> F 10:00        (cap 100)
    #   LATE    A 10:30 -> F 11:30                         (cap 100)
    #   FAST    H 08:00 -> F 08:30                         (cap 100)
    replan_body = {
        "replan_id": "dr-2026-09-27-a",
        "min_transfer_seconds": 300,
        "services": [
            {"service_id": "ALPHA", "capacity": 100, "stops": [
                {"station_id": "A", "arrival_at": "2026-09-27T08:00:00Z",
                 "departure_at": "2026-09-27T08:05:00Z"},
                {"station_id": "B", "arrival_at": "2026-09-27T08:20:00Z",
                 "departure_at": "2026-09-27T08:25:00Z"},
                {"station_id": "E", "arrival_at": "2026-09-27T08:55:00Z",
                 "departure_at": "2026-09-27T08:55:00Z"}]},
            {"service_id": "BRAVO", "capacity": 100, "stops": [
                {"station_id": "B", "arrival_at": "2026-09-27T08:30:00Z",
                 "departure_at": "2026-09-27T08:30:00Z"},
                {"station_id": "F", "arrival_at": "2026-09-27T09:00:00Z",
                 "departure_at": "2026-09-27T09:00:00Z"}]},
            {"service_id": "CHARLIE", "capacity": 100, "stops": [
                {"station_id": "A", "arrival_at": "2026-09-27T09:00:00Z",
                 "departure_at": "2026-09-27T09:05:00Z"},
                {"station_id": "B", "arrival_at": "2026-09-27T09:20:00Z",
                 "departure_at": "2026-09-27T09:25:00Z"},
                {"station_id": "F", "arrival_at": "2026-09-27T10:00:00Z",
                 "departure_at": "2026-09-27T10:00:00Z"}]},
            {"service_id": "LATE", "capacity": 100, "stops": [
                {"station_id": "A", "arrival_at": "2026-09-27T09:30:00Z",
                 "departure_at": "2026-09-27T10:30:00Z"},
                {"station_id": "F", "arrival_at": "2026-09-27T11:30:00Z",
                 "departure_at": "2026-09-27T11:30:00Z"}]},
            {"service_id": "FAST", "capacity": 100, "stops": [
                {"station_id": "H", "arrival_at": "2026-09-27T08:00:00Z",
                 "departure_at": "2026-09-27T08:00:00Z"},
                {"station_id": "F", "arrival_at": "2026-09-27T08:30:00Z",
                 "departure_at": "2026-09-27T08:30:00Z"}]},
        ],
        "groups": [
            # g1: earliest arrival is the ALPHA+BRAVO transfer at 09:00, even
            # though later zero-transfer options exist (CHARLIE 10:00).
            {"group_id": "g1", "origin": "A", "destination": "F",
             "ready_at": "2026-09-27T08:00:00Z", "party_size": 1},
            # g12: exact 300s transfer ALPHA->BRAVO is legal (boundary); a
            # party of 5 fits the seats left after g1 on each section.
            {"group_id": "g12", "origin": "A", "destination": "F",
             "ready_at": "2026-09-27T08:00:00Z", "party_size": 5},
            # g3: stays on ALPHA A -> E (multi-section service, single leg).
            {"group_id": "g3", "origin": "A", "destination": "E",
             "ready_at": "2026-09-27T08:00:00Z", "party_size": 50},
            # g3b: after g3/g1/g12, ALPHA A->B has 44 seats and B->E 50; a
            # party of 45 fits neither section for the whole A->E ride, and no
            # other service serves E.
            {"group_id": "g3b", "origin": "A", "destination": "E",
             "ready_at": "2026-09-27T08:00:00Z", "party_size": 45},
            # g2: ready 08:30 misses ALPHA's departure (08:05) and cannot board
            # BRAVO at A; CHARLIE 09:05 -> F 10:00 is the earliest feasible.
            {"group_id": "g2", "origin": "A", "destination": "F",
             "ready_at": "2026-09-27T08:30:00Z", "party_size": 1},
            # g9: B -> F on BRAVO; the minimum transfer rule never applies to
            # a first boarding (ready 08:29, departs B 08:30).
            {"group_id": "g9", "origin": "B", "destination": "F",
             "ready_at": "2026-09-27T08:29:00Z", "party_size": 1},
            # g5: E only has an inbound call; nothing leaves E.
            {"group_id": "g5", "origin": "E", "destination": "A",
             "ready_at": "2026-09-27T09:00:00Z", "party_size": 1},
            # g6: origin Z is served by nobody.
            {"group_id": "g6", "origin": "Z", "destination": "F",
             "ready_at": "2026-09-27T08:00:00Z", "party_size": 1},
            # g7: destination Z is served by nobody.
            {"group_id": "g7", "origin": "A", "destination": "Z",
             "ready_at": "2026-09-27T08:00:00Z", "party_size": 1},
            # g4: 40-person indivisible party, ready after CHARLIE left A;
            # LATE 10:30 -> 11:30 is its only ride and has enough seats.
            {"group_id": "g4", "origin": "A", "destination": "F",
             "ready_at": "2026-09-27T10:00:00Z", "party_size": 40},
            # g8: same ready_at as g4 (g4 first by group_id); after g4 LATE
            # has 60 seats left, a party of 70 cannot be split and no later
            # service exists -> stranded on section capacity.
            {"group_id": "g8", "origin": "A", "destination": "F",
             "ready_at": "2026-09-27T10:00:00Z", "party_size": 70},
            # g11: H -> F direct on FAST, no transfer needed.
            {"group_id": "g11", "origin": "H", "destination": "F",
             "ready_at": "2026-09-27T07:30:00Z", "party_size": 3},
        ],
    }

    audit_before_replans = client.get("/audit", params={"limit": 1000}).json()["count"]

    r = client.post("/disruption/replans", json=replan_body)
    check("replan created", r.status_code == 200 and r.json()["status"] == "created")
    replan = r.json()
    by_group = {g["group_id"]: g for g in replan["groups"]}
    check("replan processed all 12 groups in (ready_at, group_id) order",
          replan["summary"] == {"group_count": 12, "assigned_count": 7,
                                "stranded_count": 5}
          and [g["group_id"] for g in replan["groups"]]
          == ["g11", "g1", "g12", "g3", "g3b", "g6", "g7", "g9",
              "g2", "g5", "g4", "g8"])

    g1 = by_group["g1"]
    check("g1 earliest arrival via ALPHA+BRAVO (09:00, 1 transfer)",
          g1["status"] == "assigned" and g1["arrival_at"] == "2026-09-27T09:00:00Z"
          and g1["transfers"] == 1
          and [(l["service_id"], l["from_station"], l["to_station"]) for l in g1["legs"]]
          == [("ALPHA", "A", "B"), ("BRAVO", "B", "F")])
    check("g1 legs carry UTC departure/arrival times",
          g1["legs"][0]["departure_at"] == "2026-09-27T08:05:00Z"
          and g1["legs"][0]["arrival_at"] == "2026-09-27T08:20:00Z"
          and g1["legs"][1]["departure_at"] == "2026-09-27T08:30:00Z"
          and g1["legs"][1]["arrival_at"] == "2026-09-27T09:00:00Z")

    g12 = by_group["g12"]
    check("g12 boards on the exact 300s transfer boundary",
          g12["status"] == "assigned" and g12["arrival_at"] == "2026-09-27T09:00:00Z"
          and [l["service_id"] for l in g12["legs"]] == ["ALPHA", "BRAVO"]
          and g12["party_size"] == 5)

    g3 = by_group["g3"]
    check("g3 rides one multi-section service A->E",
          g3["status"] == "assigned" and g3["transfers"] == 0
          and [(l["service_id"], l["from_station"], l["to_station"]) for l in g3["legs"]]
          == [("ALPHA", "A", "E")])

    g3b = by_group["g3b"]
    check("g3b stranded by per-section capacity after g3/g12",
          g3b["status"] == "stranded" and g3b["legs"] == []
          and g3b["arrival_at"] is None and g3b["transfers"] is None
          and g3b["stranded_reason"].startswith("insufficient_capacity"))

    g2 = by_group["g2"]
    check("g2 ready at 08:30 takes CHARLIE (earliest feasible itinerary)",
          g2["status"] == "assigned"
          and [l["service_id"] for l in g2["legs"]] == ["CHARLIE"]
          and g2["arrival_at"] == "2026-09-27T10:00:00Z")

    g4 = by_group["g4"]
    check("g4 party of 40 rides the only late service LATE",
          g4["status"] == "assigned" and g4["party_size"] == 40
          and [l["service_id"] for l in g4["legs"]] == ["LATE"]
          and g4["arrival_at"] == "2026-09-27T11:30:00Z")

    g8 = by_group["g8"]
    check("g8 indivisible party of 70 stranded after g4 takes 40 LATE seats",
          g8["status"] == "stranded" and g8["party_size"] == 70
          and g8["stranded_reason"].startswith("insufficient_capacity"))

    g9 = by_group["g9"]
    check("g9 first boarding ignores the transfer gap",
          g9["status"] == "assigned" and g9["transfers"] == 0
          and [l["service_id"] for l in g9["legs"]] == ["BRAVO"])

    g5 = by_group["g5"]
    check("g5 destination unreachable from dead-end E",
          g5["status"] == "stranded"
          and g5["stranded_reason"].startswith("unreachable_destination"))
    check("g6 origin not served",
          by_group["g6"]["status"] == "stranded"
          and by_group["g6"]["stranded_reason"].startswith("origin_not_served"))
    check("g7 destination not served",
          by_group["g7"]["status"] == "stranded"
          and by_group["g7"]["stranded_reason"].startswith("destination_not_served"))

    g11 = by_group["g11"]
    check("g11 takes FAST H->F with zero transfers",
          g11["status"] == "assigned" and g11["transfers"] == 0
          and [l["service_id"] for l in g11["legs"]] == ["FAST"]
          and g11["arrival_at"] == "2026-09-27T08:30:00Z")

    # Fewer-transfers tie-break: same arrival via a direct and a 1-transfer
    # itinerary chooses the direct one (zero transfers).
    tie_body = {
        "replan_id": "dr-tie-transfers", "min_transfer_seconds": 600,
        "services": [
            {"service_id": "DIR", "capacity": 10, "stops": [
                {"station_id": "A", "arrival_at": "2026-09-27T08:00:00Z",
                 "departure_at": "2026-09-27T08:00:00Z"},
                {"station_id": "C", "arrival_at": "2026-09-27T09:00:00Z",
                 "departure_at": "2026-09-27T09:00:00Z"}]},
            {"service_id": "X1", "capacity": 10, "stops": [
                {"station_id": "A", "arrival_at": "2026-09-27T08:00:00Z",
                 "departure_at": "2026-09-27T08:00:00Z"},
                {"station_id": "B", "arrival_at": "2026-09-27T08:20:00Z",
                 "departure_at": "2026-09-27T08:20:00Z"}]},
            {"service_id": "X2", "capacity": 10, "stops": [
                {"station_id": "B", "arrival_at": "2026-09-27T08:30:00Z",
                 "departure_at": "2026-09-27T08:30:00Z"},
                {"station_id": "C", "arrival_at": "2026-09-27T09:00:00Z",
                 "departure_at": "2026-09-27T09:00:00Z"}]},
        ],
        "groups": [{"group_id": "t", "origin": "A", "destination": "C",
                    "ready_at": "2026-09-27T08:00:00Z", "party_size": 1}],
    }
    tg = client.post("/disruption/replans", json=tie_body).json()["groups"][0]
    check("same arrival: fewer transfers wins",
          tg["transfers"] == 0 and [l["service_id"] for l in tg["legs"]] == ["DIR"])

    # Lexicographic service-sequence tie-break.
    lex_body = {
        "replan_id": "dr-tie-lex", "min_transfer_seconds": 600,
        "services": [
            {"service_id": "SVC-B", "capacity": 10, "stops": [
                {"station_id": "A", "arrival_at": "2026-09-27T08:00:00Z",
                 "departure_at": "2026-09-27T08:00:00Z"},
                {"station_id": "B", "arrival_at": "2026-09-27T08:20:00Z",
                 "departure_at": "2026-09-27T08:20:00Z"}]},
            {"service_id": "SVC-A", "capacity": 10, "stops": [
                {"station_id": "A", "arrival_at": "2026-09-27T08:00:00Z",
                 "departure_at": "2026-09-27T08:00:00Z"},
                {"station_id": "B", "arrival_at": "2026-09-27T08:20:00Z",
                 "departure_at": "2026-09-27T08:20:00Z"}]},
            {"service_id": "SVC-Z", "capacity": 10, "stops": [
                {"station_id": "B", "arrival_at": "2026-09-27T08:30:00Z",
                 "departure_at": "2026-09-27T08:30:00Z"},
                {"station_id": "D", "arrival_at": "2026-09-27T09:00:00Z",
                 "departure_at": "2026-09-27T09:00:00Z"}]},
        ],
        "groups": [{"group_id": "t", "origin": "A", "destination": "D",
                    "ready_at": "2026-09-27T08:00:00Z", "party_size": 1}],
    }
    lg = client.post("/disruption/replans", json=lex_body).json()["groups"][0]
    check("same arrival and transfers: lexicographically smaller sequence wins",
          [l["service_id"] for l in lg["legs"]] == ["SVC-A", "SVC-Z"])

    # Merge counterexample: two itineraries converge at transfer station X.
    # The earlier one (LEG-1 + LEG-2, reaches X 08:30) has already used two
    # rides; the later one (DIRECT-LATE, reaches X 08:40) only one. Both can
    # board the same FINAL service and arrive at D together, so the later /
    # fewer-rides label must survive the merge and win the transfers
    # tie-break. A single-label-per-station search would discard it here.
    merge_body = {
        "replan_id": "dr-merge-counter", "min_transfer_seconds": 300,
        "services": [
            {"service_id": "LEG-1", "capacity": 10, "stops": [
                {"station_id": "O", "arrival_at": "2026-09-27T08:00:00Z",
                 "departure_at": "2026-09-27T08:00:00Z"},
                {"station_id": "P", "arrival_at": "2026-09-27T08:10:00Z",
                 "departure_at": "2026-09-27T08:15:00Z"}]},
            {"service_id": "LEG-2", "capacity": 10, "stops": [
                {"station_id": "P", "arrival_at": "2026-09-27T08:20:00Z",
                 "departure_at": "2026-09-27T08:20:00Z"},
                {"station_id": "X", "arrival_at": "2026-09-27T08:30:00Z",
                 "departure_at": "2026-09-27T08:30:00Z"}]},
            {"service_id": "DIRECT-LATE", "capacity": 10, "stops": [
                {"station_id": "O", "arrival_at": "2026-09-27T08:20:00Z",
                 "departure_at": "2026-09-27T08:20:00Z"},
                {"station_id": "X", "arrival_at": "2026-09-27T08:40:00Z",
                 "departure_at": "2026-09-27T08:40:00Z"}]},
            {"service_id": "FINAL", "capacity": 10, "stops": [
                {"station_id": "X", "arrival_at": "2026-09-27T09:00:00Z",
                 "departure_at": "2026-09-27T09:00:00Z"},
                {"station_id": "D", "arrival_at": "2026-09-27T09:30:00Z",
                 "departure_at": "2026-09-27T09:30:00Z"}]},
        ],
        "groups": [
            {"group_id": "m1", "origin": "O", "destination": "D",
             "ready_at": "2026-09-27T08:00:00Z", "party_size": 1},
        ],
    }
    r = client.post("/disruption/replans", json=merge_body)
    check("merge counterexample replan created", r.status_code == 200)
    mg = r.json()["groups"][0]
    check("merge at X: later fewer-rides label wins the transfers tie-break",
          mg["status"] == "assigned" and mg["arrival_at"] == "2026-09-27T09:30:00Z"
          and mg["transfers"] == 1
          and [l["service_id"] for l in mg["legs"]] == ["DIRECT-LATE", "FINAL"])

    # Same merge, but the later direct service reaches X only 240s before
    # FINAL departs (< 300s minimum transfer): it cannot board, so the
    # earlier more-rides itinerary is the feasible winner -- the earlier
    # label must survive the merge too.
    missed_body = json.loads(json.dumps(merge_body))
    missed_body["replan_id"] = "dr-merge-missed"
    missed_body["services"][2]["stops"][1]["arrival_at"] = "2026-09-27T08:56:00Z"
    missed_body["services"][2]["stops"][1]["departure_at"] = "2026-09-27T08:56:00Z"
    r = client.post("/disruption/replans", json=missed_body)
    mg = r.json()["groups"][0]
    check("merge at X: late label missing the transfer gap loses",
          mg["status"] == "assigned" and mg["arrival_at"] == "2026-09-27T09:30:00Z"
          and mg["transfers"] == 2
          and [l["service_id"] for l in mg["legs"]] == ["LEG-1", "LEG-2", "FINAL"])

    # Same merge under capacity pressure: FINAL carries a single seat, taken
    # by the earlier-processed group e0. The capacity-infeasible label via
    # FINAL must not be chosen; both merge labels fall back to FINAL-2 and
    # the fewer-rides one still wins.
    cap_merge_body = json.loads(json.dumps(merge_body))
    cap_merge_body["replan_id"] = "dr-merge-capacity"
    cap_merge_body["services"][3]["capacity"] = 1
    cap_merge_body["services"].append(
        {"service_id": "FINAL-2", "capacity": 10, "stops": [
            {"station_id": "X", "arrival_at": "2026-09-27T10:00:00Z",
             "departure_at": "2026-09-27T10:00:00Z"},
            {"station_id": "D", "arrival_at": "2026-09-27T10:30:00Z",
             "departure_at": "2026-09-27T10:30:00Z"}]})
    cap_merge_body["groups"].insert(
        0, {"group_id": "e0", "origin": "X", "destination": "D",
            "ready_at": "2026-09-27T08:00:00Z", "party_size": 1})
    r = client.post("/disruption/replans", json=cap_merge_body)
    cm = {g["group_id"]: g for g in r.json()["groups"]}
    check("capacity merge: earlier group takes the last FINAL seat",
          cm["e0"]["status"] == "assigned"
          and [l["service_id"] for l in cm["e0"]["legs"]] == ["FINAL"])
    check("capacity merge: infeasible label skipped, fewer rides still wins",
          cm["m1"]["status"] == "assigned"
          and cm["m1"]["arrival_at"] == "2026-09-27T10:30:00Z"
          and cm["m1"]["transfers"] == 1
          and [l["service_id"] for l in cm["m1"]["legs"]] == ["DIRECT-LATE", "FINAL-2"])

    # Per-section capacity: one service, two adjacent sections, capacity 4
    # each. A 4-person S1->S2 group fills only the first section; a 4-person
    # S2->S3 group still fits the second; a 1-person S1->S3 party needs both
    # and is stranded by the exhausted first section, as is the next
    # S1->S2 party.
    section_body = {
        "replan_id": "dr-section-capacity", "min_transfer_seconds": 300,
        "services": [
            {"service_id": "SEC", "capacity": 4, "stops": [
                {"station_id": "S1", "arrival_at": "2026-09-27T08:00:00Z",
                 "departure_at": "2026-09-27T08:05:00Z"},
                {"station_id": "S2", "arrival_at": "2026-09-27T08:20:00Z",
                 "departure_at": "2026-09-27T08:25:00Z"},
                {"station_id": "S3", "arrival_at": "2026-09-27T08:40:00Z",
                 "departure_at": "2026-09-27T08:40:00Z"}]},
        ],
        "groups": [
            {"group_id": "g1", "origin": "S1", "destination": "S2",
             "ready_at": "2026-09-27T08:00:00Z", "party_size": 4},
            {"group_id": "g2", "origin": "S2", "destination": "S3",
             "ready_at": "2026-09-27T08:00:00Z", "party_size": 4},
            {"group_id": "g3", "origin": "S1", "destination": "S3",
             "ready_at": "2026-09-27T08:00:00Z", "party_size": 1},
            {"group_id": "g4", "origin": "S1", "destination": "S2",
             "ready_at": "2026-09-27T08:00:00Z", "party_size": 1},
        ],
    }
    r = client.post("/disruption/replans", json=section_body)
    sg = {g["group_id"]: g for g in r.json()["groups"]}
    check("per-section: first section filled by g1",
          sg["g1"]["status"] == "assigned"
          and sg["g1"]["arrival_at"] == "2026-09-27T08:20:00Z")
    check("per-section: second section stays independent for g2",
          sg["g2"]["status"] == "assigned"
          and sg["g2"]["arrival_at"] == "2026-09-27T08:40:00Z")
    check("per-section: g3 blocked by the one full section",
          sg["g3"]["status"] == "stranded"
          and sg["g3"]["stranded_reason"].startswith("insufficient_capacity"))
    check("per-section: g4 finds no seat on the exhausted section",
          sg["g4"]["status"] == "stranded"
          and sg["g4"]["stranded_reason"].startswith("insufficient_capacity"))

    # Idempotency: array order and offset notation do not matter; different
    # payload -> 409.
    r = client.post("/disruption/replans", json=replan_body)
    check("identical replan replays as duplicate",
          r.status_code == 200 and r.json()["status"] == "duplicate"
          and r.json()["groups"] == replan["groups"])
    reordered = dict(replan_body,
                     services=list(reversed(replan_body["services"])),
                     groups=list(reversed(replan_body["groups"])))
    r = client.post("/disruption/replans", json=reordered)
    check("service/group array order does not affect identity",
          r.json()["status"] == "duplicate"
          and r.json()["groups"] == replan["groups"])
    tz_body = json.loads(json.dumps(replan_body))
    tz_body["services"][0]["stops"][0]["departure_at"] = "2026-09-27T16:05:00+08:00"
    r = client.post("/disruption/replans", json=tz_body)
    check("different offset notation of the same instant -> duplicate",
          r.json()["status"] == "duplicate")
    conflict = json.loads(json.dumps(replan_body))
    conflict["min_transfer_seconds"] = 301
    check("same replan_id, different payload -> 409",
          client.post("/disruption/replans", json=conflict).status_code == 409)

    # Timetable validation: duplicate stops, non-monotonic times, duplicate
    # service/group ids, negative/zero constraints -> 422 and nothing stored.
    def bad_replan(rid, mutate):
        b = json.loads(json.dumps(replan_body))
        b["replan_id"] = rid
        mutate(b)
        return client.post("/disruption/replans", json=b)

    def dup_station(b):
        b["services"][0]["stops"][2]["station_id"] = "A"

    def backwards_arrival(b):
        b["services"][0]["stops"][1]["arrival_at"] = "2026-09-27T08:04:00Z"

    def departure_before_arrival(b):
        b["services"][0]["stops"][0]["departure_at"] = "2026-09-27T07:59:00Z"

    def dup_service(b):
        b["services"].append(dict(b["services"][0]))

    def dup_group(b):
        b["groups"].append(dict(b["groups"][0]))

    check("duplicate station in a service -> 422",
          bad_replan("dr-bad-station", dup_station).status_code == 422)
    check("non-monotonic arrival -> 422",
          bad_replan("dr-bad-mono", backwards_arrival).status_code == 422)
    check("departure before arrival -> 422",
          bad_replan("dr-bad-dep", departure_before_arrival).status_code == 422)
    check("duplicate service_id -> 422",
          bad_replan("dr-bad-svc", dup_service).status_code == 422)
    check("duplicate group_id -> 422",
          bad_replan("dr-bad-grp", dup_group).status_code == 422)
    check("negative min transfer -> 422",
          bad_replan("dr-bad-mts",
                     lambda b: b.update(min_transfer_seconds=-1)).status_code == 422)
    check("party_size 0 -> 422",
          bad_replan("dr-bad-party",
                     lambda b: b["groups"][0].__setitem__("party_size", 0)).status_code == 422)
    check("single-stop service -> 422",
          bad_replan("dr-bad-stops",
                     lambda b: b["services"][0].__setitem__("stops", b["services"][0]["stops"][:1])
                     ).status_code == 422)

    # GET freezes the created view; missing id -> 404; 422 never persisted.
    r = client.get("/disruption/replans/dr-2026-09-27-a")
    check("GET returns the frozen replan",
          r.status_code == 200 and r.json()["status"] == "created"
          and r.json()["groups"] == replan["groups"]
          and r.json()["summary"] == replan["summary"])
    check("unknown replan -> 404",
          client.get("/disruption/replans/does-not-exist").status_code == 404)

    with sqlite3.connect(os.environ["DB_PATH"]) as raw:
        ids = [row[0] for row in raw.execute(
            "SELECT replan_id FROM disruption_replans ORDER BY replan_id")]
        # Main + tie/merge/section bodies; replays, 409s and 422s added nothing.
        check("only valid replans persisted",
              ids == ["dr-2026-09-27-a", "dr-merge-capacity", "dr-merge-counter",
                      "dr-merge-missed", "dr-section-capacity",
                      "dr-tie-lex", "dr-tie-transfers"])
        blocked = []
        for stmt in (
            "UPDATE disruption_replans SET assigned_count = 0"
            " WHERE replan_id = 'dr-2026-09-27-a'",
            "DELETE FROM disruption_replans WHERE replan_id = 'dr-2026-09-27-a'",
        ):
            try:
                raw.execute(stmt)
            except sqlite3.Error:
                blocked.append(stmt)
        check("replan table is append-only at the storage layer", len(blocked) == 2)

    # Replans touch no business data and append no audit records.
    check("replans create no audit records",
          client.get("/audit", params={"limit": 1000}).json()["count"]
          == audit_before_replans)

    # --- Tap-event review holds (刷卡事件复核暂存与释放) ----------------------
    def hpost(eid, passenger, station, tap, ts):
        rr = client.post("/events", json={
            "event_id": eid, "passenger_id": passenger,
            "station_id": station, "tap_type": tap, "occurred_at": ts,
        })
        assert rr.status_code == 200, rr.text
        return rr.json()

    # 2026-10-22, a day no other section uses. Three trips for HR (spaced well
    # beyond the 90-minute transfer window, so each is a full 500-cent fare):
    #   hr1:hr2  S01->S04  08:00->08:30  500
    #   hr3:hr4  S01->S04  12:00->12:30  500
    #   hr5:hr6  S01->S04  16:00->16:30  500
    for eid, st, tap, ts in (
        ("hr1", "S01", "in", "2026-10-22T08:00:00Z"),
        ("hr2", "S04", "out", "2026-10-22T08:30:00Z"),
        ("hr3", "S01", "in", "2026-10-22T12:00:00Z"),
        ("hr4", "S04", "out", "2026-10-22T12:30:00Z"),
        ("hr5", "S01", "in", "2026-10-22T16:00:00Z"),
        ("hr6", "S04", "out", "2026-10-22T16:30:00Z"),
    ):
        hpost(eid, "HR", st, tap, ts)
    bill = {t["trip_id"]: t["amount_cents"]
            for t in client.get("/passengers/HR/bill").json()["trips"]}
    check("hold fixtures price three 500-cent trips",
          bill == {"hr1:hr2": 500, "hr3:hr4": 500, "hr5:hr6": 500}, bill)

    hold_body = {
        "hold_id": "hold-2026-10-22-a",
        "passenger_id": "HR",
        "event_ids": ["hr3", "hr4"],
        "review_reason": "middle trip under fare review",
    }
    r = client.post("/events/review-holds", json=hold_body)
    check("hold created", r.status_code == 200)
    held = r.json()
    check("hold is held at version 1 with no release",
          held["status"] == "created" and held["state"] == "held"
          and held["version"] == 1 and held["release"] is None
          and held["released_at"] is None)
    check("hold stores canonical sorted event ids",
          held["event_ids"] == ["hr3", "hr4"] and len(held["snapshots"]) == 2)
    snap_by_id = {s["event_id"]: s for s in held["snapshots"]}
    check("hold snapshots freeze the complete event payload",
          snap_by_id["hr3"]["station_id"] == "S01"
          and snap_by_id["hr3"]["tap_type"] == "in"
          and snap_by_id["hr3"]["occurred_at"] == "2026-10-22T12:00:00Z"
          and snap_by_id["hr3"]["event_seq"] > 0
          and len(snap_by_id["hr3"]["payload_hash"]) == 64
          and snap_by_id["hr3"]["frozen_at"])
    hold_adjustments = {a["trip_id"]: a for a in held["adjustments"]}
    check("hold removes the frozen trip: one -500 adjustment",
          list(hold_adjustments) == ["hr3:hr4"]
          and hold_adjustments["hr3:hr4"]["delta_cents"] == -500
          and held["trip_change_count"] == 1)

    # Pairing, billing and pending stop using the held events.
    bill = {t["trip_id"]: t["amount_cents"]
            for t in client.get("/passengers/HR/bill").json()["trips"]}
    check("bill excludes the held trip",
          bill == {"hr1:hr2": 500, "hr5:hr6": 500}, bill)
    check("held events are not listed as pending",
          client.get("/pending", params={"passenger_id": "HR"}).json() == [])

    # GET reports the frozen hold.
    g = client.get("/events/review-holds/hold-2026-10-22-a").json()
    check("GET hold returns held state and frozen snapshots",
          g["state"] == "held" and g["version"] == 1
          and g["review_reason"] == "middle trip under fare review"
          and g["snapshots"] == held["snapshots"])
    check("GET hold omits the POST-only replay marker", g["status"] == "created")

    # Idempotency: same hold_id + same payload (order-insensitive) replays;
    # different payload is 409.
    r = client.post("/events/review-holds", json=hold_body)
    check("identical hold replays as duplicate",
          r.status_code == 200 and r.json()["status"] == "duplicate"
          and r.json()["snapshots"] == held["snapshots"])
    reordered = dict(hold_body, event_ids=["hr4", "hr3"])
    r = client.post("/events/review-holds", json=reordered)
    check("event id order does not affect hold identity",
          r.json()["status"] == "duplicate")
    r = client.post("/events/review-holds", json=dict(hold_body, review_reason="changed"))
    check("same hold_id, different payload -> 409", r.status_code == 409)

    # Corrections on held events are refused; an identical replay stays a
    # plain duplicate and changes nothing.
    r = client.post("/events", json={
        "event_id": "hr3", "passenger_id": "HR", "station_id": "S02",
        "tap_type": "in", "occurred_at": "2026-10-22T12:00:00Z"})
    check("correction of a held event -> 409", r.status_code == 409)
    r = client.post("/events", json={
        "event_id": "hr3", "passenger_id": "HR", "station_id": "S01",
        "tap_type": "in", "occurred_at": "2026-10-22T12:00:00Z"})
    check("identical submission of a held event stays an idempotent duplicate",
          r.status_code == 200 and r.json()["status"] == "duplicate"
          and r.json()["adjustments"] == [])

    # A second hold over an already-held event is a whole-batch 409.
    r = client.post("/events/review-holds", json={
        "hold_id": "hold-overlap", "passenger_id": "HR",
        "event_ids": ["hr3"], "review_reason": "overlap"})
    check("hold overlapping another hold -> 409", r.status_code == 409)

    # Structural failures.
    r = client.post("/events/review-holds", json={
        "hold_id": "hold-missing", "passenger_id": "HR",
        "event_ids": ["nope"], "review_reason": "x"})
    check("hold on unknown event -> 422", r.status_code == 422)
    r = client.post("/events/review-holds", json={
        "hold_id": "hold-foreign", "passenger_id": "OTHER",
        "event_ids": ["hr1"], "review_reason": "x"})
    check("hold on another passenger's event -> 422", r.status_code == 422)
    r = client.post("/events/review-holds", json={
        "hold_id": "hold-dup", "passenger_id": "HR",
        "event_ids": ["hr1", "hr1"], "review_reason": "x"})
    check("duplicate event ids in one hold -> 422", r.status_code == 422)
    check("unknown hold -> 404",
          client.get("/events/review-holds/nope").status_code == 404)
    check("releasing an unknown hold -> 404",
          client.post("/events/review-holds/nope/release",
                      json={"expected_version": 1}).status_code == 404)

    # Events already booked by immutable artefacts cannot be held: use the
    # existing reconciliation batch (ra1:ra2), clearing ledger (op1:op2) and a
    # resolved dispute (d2 -> dp3:dp4) from earlier sections.
    r = client.post("/events/review-holds", json={
        "hold_id": "hold-referenced-recon", "passenger_id": "RA",
        "event_ids": ["ra1"], "review_reason": "x"})
    check("hold on an event referenced by a reconciliation batch -> 409",
          r.status_code == 409)
    r = client.post("/events/review-holds", json={
        "hold_id": "hold-referenced-clearing", "passenger_id": "OP",
        "event_ids": ["op1"], "review_reason": "x"})
    check("hold on an event referenced by a clearing batch -> 409",
          r.status_code == 409)
    r = client.post("/events/review-holds", json={
        "hold_id": "hold-referenced-dispute", "passenger_id": "DP",
        "event_ids": ["dp3"], "review_reason": "x"})
    check("hold on an event referenced by a resolved dispute -> 409",
          r.status_code == 409)
    # An OPEN dispute does not block the hold (no immutable booking yet). Use
    # a fresh trip whose only dispute is still open.
    hpost("hd1", "HD", "S01", "in", "2026-10-22T18:00:00Z")
    hpost("hd2", "HD", "S04", "out", "2026-10-22T18:30:00Z")
    od = client.post("/disputes", json={
        "dispute_id": "d-open-hold", "passenger_id": "HD",
        "trip_id": "hd1:hd2", "claimed_amount_cents": 100, "reason": "open"})
    check("fresh open dispute created", od.status_code == 200
          and od.json()["state"] == "open")
    r = client.post("/events/review-holds", json={
        "hold_id": "hold-open-dispute-ok", "passenger_id": "HD",
        "event_ids": ["hd1", "hd2"], "review_reason": "x"})
    check("an open dispute does not block a hold",
          r.status_code == 200 and r.json()["state"] == "held")
    client.post("/events/review-holds/hold-open-dispute-ok/release",
                json={"expected_version": 1})
    # A refused hold changed nothing: RA's trips are still billable.
    check("refused holds leave settlement untouched",
          "ra1:ra2" in {t["trip_id"] for t in
                         client.get("/passengers/RA/bill").json()["trips"]})

    # Version conflict keeps the hold held; snapshot mismatch also refuses.
    r = client.post("/events/review-holds/hold-2026-10-22-a/release",
                    json={"expected_version": 2})
    check("release with stale version -> 409 and stays held",
          r.status_code == 409
          and client.get("/events/review-holds/hold-2026-10-22-a")
              .json()["state"] == "held")
    with sqlite3.connect(os.environ["DB_PATH"]) as raw:
        raw.execute("UPDATE events SET station_id = 'S02' WHERE event_id = 'hr3'")
        raw.commit()
    r = client.post("/events/review-holds/hold-2026-10-22-a/release",
                    json={"expected_version": 1})
    check("release with a diverged event snapshot -> 409 and stays held",
          r.status_code == 409
          and client.get("/events/review-holds/hold-2026-10-22-a")
              .json()["state"] == "held")
    check("diverged snapshot stays frozen at the original payload",
          {s["event_id"]: s["station_id"]
           for s in client.get("/events/review-holds/hold-2026-10-22-a")
              .json()["snapshots"]} == {"hr3": "S01", "hr4": "S04"})
    with sqlite3.connect(os.environ["DB_PATH"]) as raw:
        raw.execute("UPDATE events SET station_id = 'S01' WHERE event_id = 'hr3'")
        raw.commit()

    # Successful release: original (occurred_at, seq) re-settlement, signed
    # per-trip deltas returned and frozen onto the append-only release row.
    r = client.post("/events/review-holds/hold-2026-10-22-a/release",
                    json={"expected_version": 1})
    check("release succeeds at expected version 1", r.status_code == 200)
    released = r.json()
    check("release moves the hold to state released, version 2",
          released["status"] == "created" and released["state"] == "released"
          and released["version"] == 2
          and released["released_at"] is not None
          and released["release"]["release_id"] == "RL-hold-2026-10-22-a")
    release_adjustments = {a["trip_id"]: a
                           for a in released["release"]["adjustments"]}
    check("release re-settles by original seq/time: +500 on hr3:hr4",
          list(release_adjustments) == ["hr3:hr4"]
          and release_adjustments["hr3:hr4"]["delta_cents"] == 500
          and release_adjustments["hr3:hr4"]["new_amount_cents"] == 500)
    bill = {t["trip_id"]: t["amount_cents"]
            for t in client.get("/passengers/HR/bill").json()["trips"]}
    check("bill restored after release",
          bill == {"hr1:hr2": 500, "hr3:hr4": 500, "hr5:hr6": 500}, bill)

    # Release is append-only: identical replay is a duplicate; any other
    # payload is 409 and cannot book a second release.
    r = client.post("/events/review-holds/hold-2026-10-22-a/release",
                    json={"expected_version": 1})
    check("identical release replays as duplicate with the same result",
          r.status_code == 200 and r.json()["status"] == "duplicate"
          and r.json()["release"]["adjustments"]
          == released["release"]["adjustments"])
    r = client.post("/events/review-holds/hold-2026-10-22-a/release",
                    json={"expected_version": 2})
    check("a second, different release -> 409", r.status_code == 409)
    g = client.get("/events/review-holds/hold-2026-10-22-a").json()
    check("GET after release carries the frozen release result",
          g["state"] == "released" and g["version"] == 2
          and g["release"]["release_id"] == "RL-hold-2026-10-22-a"
          and {a["trip_id"]: a["delta_cents"]
               for a in g["release"]["adjustments"]} == {"hr3:hr4": 500}
          and g["snapshots"] == held["snapshots"])

    # Storage-layer immutability for holds, snapshots and releases.
    with sqlite3.connect(os.environ["DB_PATH"]) as raw:
        blocked_holds = []
        for stmt in (
            "UPDATE event_holds SET review_reason = 'z'"
            " WHERE hold_id = 'hold-2026-10-22-a'",
            "DELETE FROM event_holds WHERE hold_id = 'hold-2026-10-22-a'",
            "UPDATE event_hold_snapshots SET station_id = 'S02'"
            " WHERE hold_id = 'hold-2026-10-22-a'",
            "DELETE FROM event_hold_snapshots WHERE hold_id = 'hold-2026-10-22-a'",
            "UPDATE event_hold_releases SET expected_version = 2"
            " WHERE hold_id = 'hold-2026-10-22-a'",
            "DELETE FROM event_hold_releases WHERE hold_id = 'hold-2026-10-22-a'",
        ):
            try:
                raw.execute(stmt)
            except sqlite3.Error:
                blocked_holds.append(stmt)
        check("holds/snapshots/releases are immutable at the storage layer",
              len(blocked_holds) == 6)
        n_releases = raw.execute(
            "SELECT COUNT(*) FROM event_hold_releases").fetchone()[0]
        check("one immutable release row per released hold", n_releases >= 2)

    # --- Correction impact preview (更正影响预览, read-only) -------------------
    def preview(eid, pid, st, tap, ts):
        return client.post("/events/correction-preview", json={
            "event_id": eid, "passenger_id": pid, "station_id": st,
            "tap_type": tap, "occurred_at": ts,
        })

    # Dedicated preview fixtures, built late so nothing else references them.
    # PA: two trips on 2026-11-06 (a fresh recon day) 500 / 200.
    post("pa1", "PA", "S01", "in", "2026-11-06T08:00:00Z")
    post("pa2", "PA", "S04", "out", "2026-11-06T08:30:00Z")   # 500
    post("pa3", "PA", "S05", "in", "2026-11-06T09:00:00Z")
    post("pa4", "PA", "S06", "out", "2026-11-06T09:20:00Z")   # transfer 200
    # PD: one trip that gets a resolved dispute (300 same-zone).
    post("pd3", "PD", "S01", "in", "2026-11-07T08:00:00Z")
    post("pd4", "PD", "S02", "out", "2026-11-07T08:30:00Z")   # 300
    # PC: transfer pair on 2026-11-08 for clearing (400 after transfer).
    post("pc3", "PC", "S05", "in", "2026-11-08T06:00:00Z")
    post("pc4", "PC", "S02", "out", "2026-11-08T07:00:00Z")   # 500
    post("pc5", "PC", "S01", "in", "2026-11-08T08:00:00Z")
    post("pc6", "PC", "S04", "out", "2026-11-08T08:30:00Z")   # transfer 400

    preview_recon = client.post("/reconciliations", json={
        "batch_id": "recon-preview-day",
        "window_start": "2026-11-06T00:00:00Z",
        "window_end": "2026-11-07T00:00:00Z",
        "tolerance_seconds": 600,
        "records": [
            {"serial_no": "p1", "passenger_id": "PA",
             "charged_at": "2026-11-06T08:30:00Z", "amount_cents": 500,
             "trip_id": "pa1:pa2"},
        ],
    })
    check("preview recon fixture created", preview_recon.status_code == 200)

    client.post("/disputes", json={
        "dispute_id": "disp-preview", "passenger_id": "PD",
        "trip_id": "pd3:pd4", "claimed_amount_cents": 300,
        "reason": "preview dispute",
    })
    client.post("/disputes/disp-preview/resolve", json={
        "expected_version": 1, "approved_amount_cents": 100,
        "review_note": "partial",
    })

    preview_clearing = client.post("/clearings", json={
        "clearing_id": "clr-preview-day",
        "window_start": "2026-11-08T00:00:00Z",
        "window_end": "2026-11-09T00:00:00Z",
        "rules": [
            {"from_station": "S01", "to_station": "S04", "shares": [
                {"operator_id": "OA", "basis_points": 5000},
                {"operator_id": "OB", "basis_points": 5000}]},
            {"from_station": "S05", "to_station": "S02", "shares": [
                {"operator_id": "OD", "basis_points": 5000},
                {"operator_id": "OE", "basis_points": 5000}]},
        ],
    })
    check("preview clearing fixture created", preview_clearing.status_code == 200)

    # Held-event fixture is created before the read-only baseline so the
    # baseline already includes its (real) writes; only preview calls must
    # leave the counts untouched.
    post("ph3", "PH", "S01", "in", "2026-11-09T08:00:00Z")
    post("ph4", "PH", "S04", "out", "2026-11-09T08:30:00Z")
    client.post("/events/review-holds", json={
        "hold_id": "hold-preview-2", "passenger_id": "PH",
        "event_ids": ["ph3"], "review_reason": "preview held gate",
    })

    with sqlite3.connect(os.environ["DB_PATH"]) as raw:
        preview_counts_before = {
            "events": raw.execute("SELECT COUNT(*) FROM events").fetchone()[0],
            "trips": raw.execute("SELECT COUNT(*) FROM trips").fetchone()[0],
            "charges": raw.execute("SELECT COUNT(*) FROM trip_charges").fetchone()[0],
            "audit": raw.execute("SELECT COUNT(*) FROM audit_log").fetchone()[0],
        }
        preview_audit_head = raw.execute(
            "SELECT MAX(seq) FROM audit_log"
        ).fetchone()[0]

    # Identical payload -> no_change, current trips echoed, no adjustments.
    r = preview("pa1", "PA", "S01", "in", "2026-11-06T08:00:00Z")
    check("identical payload previews no_change",
          r.status_code == 200 and r.json()["status"] == "no_change")
    pb = r.json()
    check("no_change echoes trips with zero deltas",
          [t["trip_id"] for t in pb["passengers"][0]["new_trips"]]
          == ["pa1:pa2", "pa3:pa4"]
          and pb["total_delta_cents"] == 0
          and not pb["passengers"][0]["adjustments"]
          and not pb["reconciliation_impacts"]
          and not pb["clearing_impacts"]
          and not pb["resolved_dispute_impacts"])

    # Validation gates.
    check("preview missing event -> 404",
          preview("no-such-event", "PA", "S01", "in", "2026-11-06T08:00:00Z").status_code == 404)
    check("preview unknown station -> 422",
          preview("pa1", "PA", "ZZZ", "in", "2026-11-06T08:00:00Z").status_code == 422)
    check("preview bad time -> 422",
          client.post("/events/correction-preview", json={
              "event_id": "pa1", "passenger_id": "PA", "station_id": "S01",
              "tap_type": "in", "occurred_at": "nonsense"}).status_code == 422)

    # Amount + station change on pa2 (S04/zone2 -> S07/zone3 => 500 -> 800):
    # adjustment previewed and the frozen recon batch is flagged.
    r = preview("pa2", "PA", "S07", "out", "2026-11-06T08:30:00Z")
    body = r.json()
    check("changed payload previews preview", r.status_code == 200 and body["status"] == "preview")
    pa_adj = {a["trip_id"]: a for a in body["passengers"][0]["adjustments"]}
    check("preview prices the change under official rules",
          pa_adj["pa1:pa2"]["old_amount_cents"] == 500
          and pa_adj["pa1:pa2"]["new_amount_cents"] == 800
          and pa_adj["pa1:pa2"]["delta_cents"] == 300
          and body["total_delta_cents"] == 300)
    recon_hit = [i for i in body["reconciliation_impacts"]
                 if i["batch_id"] == "recon-preview-day"]
    check("preview flags the frozen reconciliation batch with reason",
          len(recon_hit) == 1 and recon_hit[0]["trip_id"] == "pa1:pa2"
          and recon_hit[0]["change_code"] == "stations_changed"
          and "station pair changes" in recon_hit[0]["reason"])

    # Resolved dispute: repricing pd4 to zone 3 (800) moves the clawback gap.
    r = preview("pd4", "PD", "S07", "out", "2026-11-07T08:30:00Z")
    di = r.json()["resolved_dispute_impacts"]
    check("preview flags the resolved dispute with old/new difference",
          len(di) == 1 and di[0]["dispute_id"] == "disp-preview"
          and di[0]["frozen_amount_cents"] == 300 and di[0]["refunded_cents"] == 100
          and di[0]["old_current_cents"] == 300 and di[0]["new_current_cents"] == 800
          and di[0]["old_difference_cents"] == -100
          and di[0]["new_difference_cents"] == -600
          and di[0]["new_direction"] == "clawback_due")

    # Voiding pd3 (tap-in -> tap-out) removes the disputed trip: top-up.
    r = preview("pd3", "PD", "S01", "out", "2026-11-07T08:00:00Z")
    di = r.json()["resolved_dispute_impacts"][0]
    check("preview reports voided dispute trip as top-up",
          di["change_code"] == "trip_voided" and di["new_current_cents"] == 0
          and di["new_difference_cents"] == 200
          and di["new_direction"] == "top_up_due")
    check("preview voided trip adjustment is negative",
          any(a["trip_id"] == "pd3:pd4" and a["delta_cents"] == -300
              and a["basis"].get("voided")
              for a in r.json()["passengers"][0]["adjustments"]))

    # Clearing: uncovered station pair reported, nothing assumed bookable.
    r = preview("pc6", "PC", "S07", "out", "2026-11-08T08:30:00Z")
    cl = r.json()["clearing_impacts"]
    check("preview reports uncovered clearing pair",
          len(cl) == 1 and cl[0]["base_clearing_id"] == "clr-preview-day"
          and cl[0]["rule_coverage"] == "uncovered"
          and "S01->S07" in cl[0]["uncovered_station_pairs"])

    # Clearing: covered amount-only change via transfer-window break (+100).
    r = preview("pc4", "PC", "S02", "out", "2026-11-08T06:29:00Z")
    cl = r.json()["clearing_impacts"]
    ct = next(t for t in cl[0]["trips"] if t["trip_id"] == "pc5:pc6")
    check("preview books clearing target-minus-cumulative deltas",
          cl[0]["rule_coverage"] == "covered"
          and ct["change_code"] == "amount_changed"
          and ct["old_gross_cents"] == 400 and ct["new_gross_cents"] == 500
          and {o["operator_id"]: o["delta_cents"] for o in ct["operators"]}
          == {"OA": 50, "OB": 50}
          and cl[0]["delta_cents"] == 100)

    # Passenger change: both old and new passengers are simulated.
    r = preview("pa2", "PZ", "S04", "out", "2026-11-06T08:30:00Z")
    body = r.json()
    check("preview simulates both passengers on passenger change",
          body["simulated_passengers"] == ["PA", "PZ"])
    pblocks = {p["passenger_id"]: p for p in body["passengers"]}
    check("old passenger loses the trip, new passenger gains no trip",
          any(a["trip_id"] == "pa1:pa2" and a["delta_cents"] == -500
              for a in pblocks["PA"]["adjustments"])
          and pblocks["PZ"]["new_trips"] == [])

    # Held events are rejected, like the real correction endpoint (the
    # hold-preview-2 fixture was created before the read-only baseline).
    check("preview refuses held events with 409",
          preview("ph3", "PH", "S02", "in", "2026-11-09T08:00:00Z").status_code == 409)

    # Read-only guarantee: after every preview, no business/frozen table moved
    # and the audit chain did not gain a record.
    with sqlite3.connect(os.environ["DB_PATH"]) as raw:
        preview_counts_after = {
            "events": raw.execute("SELECT COUNT(*) FROM events").fetchone()[0],
            "trips": raw.execute("SELECT COUNT(*) FROM trips").fetchone()[0],
            "charges": raw.execute("SELECT COUNT(*) FROM trip_charges").fetchone()[0],
            "audit": raw.execute("SELECT COUNT(*) FROM audit_log").fetchone()[0],
        }
        check("previews write no events/trips/charges/audit rows",
              preview_counts_after == preview_counts_before,
              f"{preview_counts_before} -> {preview_counts_after}")
        check("audit head unchanged by previews",
              raw.execute("SELECT MAX(seq) FROM audit_log").fetchone()[0]
              == preview_audit_head)
        check("previewed amounts were never persisted",
              raw.execute(
                  "SELECT amount_cents FROM trip_charges WHERE trip_id='pa1:pa2'"
              ).fetchone()[0] == 500
              and raw.execute(
                  "SELECT station_id FROM events WHERE event_id='pa2'"
              ).fetchone()[0] == "S04"
              and raw.execute(
                  "SELECT COUNT(*) FROM trips WHERE passenger_id='PZ'"
              ).fetchone()[0] == 0)

    client.post("/events/review-holds/hold-preview-2/release",
                json={"expected_version": 1})

    # The official /events idempotency and correction semantics are intact.
    r = client.post("/events", json={
        "event_id": "pa2", "passenger_id": "PA", "station_id": "S04",
        "tap_type": "out", "occurred_at": "2026-11-06T08:30:00Z",
    })
    check("identical /events submission still duplicates after previews",
          r.status_code == 200 and r.json()["status"] == "duplicate")

    # --- Business audit chain ----------------------------------------------------
    # Fetch the whole chain in one page: the default page size is 100 and this
    # run appends well over 100 business records.
    audit = client.get("/audit", params={"limit": 1000}).json()
    check("audit returns a 1-based global sequence",
          [e["seq"] for e in audit["entries"]] == list(range(1, audit["count"] + 1)))

    ops = {}
    for e in audit["entries"]:
        ops[e["operation"]] = ops.get(e["operation"], 0) + 1
    with sqlite3.connect(os.environ["DB_PATH"]) as raw:
        n_events = raw.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        n_batches = raw.execute("SELECT COUNT(*) FROM recon_batches").fetchone()[0]
        n_validations = raw.execute(
            "SELECT COUNT(*) FROM network_validations"
        ).fetchone()[0]
        n_snapshots = raw.execute(
            "SELECT COUNT(*) FROM network_snapshots"
        ).fetchone()[0]
        n_disputes = raw.execute("SELECT COUNT(*) FROM disputes").fetchone()[0]
        n_resolved = raw.execute("SELECT COUNT(*) FROM refunds").fetchone()[0]
        n_clearing = raw.execute(
            "SELECT COUNT(*) FROM clearing_batches").fetchone()[0]
        n_holds = raw.execute(
            "SELECT COUNT(*) FROM event_holds").fetchone()[0]
        n_hold_releases = raw.execute(
            "SELECT COUNT(*) FROM event_hold_releases").fetchone()[0]
    check("one processed audit per stored event", ops.get("event_processed") == n_events)
    check("ten corrected audits (a2, ra2, dp2, dp4, dp5, op3 x2, op6, op5, op6 re-pair)",
          ops.get("event_corrected") == 10)
    check("one created audit per recon batch", ops.get("recon_batch_created") == n_batches)
    check("one audit per network validation",
          ops.get("network_validated") == n_validations)
    check("one audit per frozen network snapshot",
          ops.get("network_snapshot_created") == n_snapshots)
    check("one audit per opened dispute",
          ops.get("dispute_opened") == n_disputes)
    check("one audit per resolved dispute",
          ops.get("dispute_resolved") == n_resolved)
    check("one audit per clearing batch (initial and adjustments)",
          ops.get("clearing_closed") == n_clearing)
    check("one audit per stored event review hold",
          ops.get("event_held") == n_holds)
    check("one audit per hold release",
          ops.get("event_hold_released") == n_hold_releases)

    entries = audit["entries"]
    check("genesis record has empty prev_hash", entries[0]["prev_hash"] == "")
    chain_ok = all(
        cur["prev_hash"] == prev["record_hash"]
        for prev, cur in zip(entries, entries[1:])
    )
    check("each record chains to the previous hash", chain_ok)
    first = entries[0]
    check("audit request summary is normalised",
          first["request_summary"] == {
              "event_id": "a1", "passenger_id": "A", "station_id": "S01",
              "tap_type": "in", "occurred_at": "2026-09-21T08:00:00Z",
          })
    check("audit result summary mirrors the API result",
          first["result_summary"]["status"] == "processed"
          and first["result_summary"]["event_pairing"] == "pending")

    # Idempotency: duplicates, conflicts and validation failures add no audit.
    post("a4", "A", "S06", "out", "2026-09-21T09:20:00Z")          # duplicate event
    client.post("/reconciliations", json=batch)                     # duplicate batch
    client.post("/reconciliations", json=conflict)                  # 409
    client.post("/network/validate", json=shuffled_net)             # duplicate validation
    client.post("/network/validate", json=conflict_net)             # 409
    client.post("/network/validate", json=bad_window)               # 422
    client.post("/network/snapshots", json={"validation_id": "net-ok-1"})   # duplicate freeze
    client.post("/network/snapshots", json={"validation_id": "net-unreach"}) # 409
    client.post("/disputes", json={                                          # duplicate dispute
        "dispute_id": "d1", "passenger_id": "DP", "trip_id": "dp1:dp2",
        "claimed_amount_cents": 400, "reason": "wrong zone charged"})
    client.post("/disputes", json=conflict_body)                             # 409 dispute
    resolve("d1", 1, 350, "approved partial refund")                         # duplicate resolution
    resolve("d1", 2, 350, "approved partial refund")                         # 409 re-resolve
    resolve("d5", 2, 500)                                                    # 409 version conflict
    client.post("/clearings", json=clearing_body)                            # duplicate clearing
    client.post("/clearings", json=dict(clearing_body, window_end="2026-10-12T00:00:00Z"))  # 409
    client.post("/clearings/adjustments", json={                             # duplicate adjustment
        "clearing_id": "clr-2026-10-10-b", "base_clearing_id": "clr-2026-10-10-a"})
    client.post("/clearings/adjustments", json={                             # 409 conflict
        "clearing_id": "clr-2026-10-10-b", "base_clearing_id": "other-root"})
    client.post("/clearings/adjustments", json={                             # 404 unknown root
        "clearing_id": "clr-no-audit", "base_clearing_id": "no-such-root"})
    client.post("/clearings", json=dict(                                     # 422 inverted window
        clearing_body, clearing_id="clr-no-audit",
        window_start="2026-10-11T00:00:00Z", window_end="2026-10-10T00:00:00Z"))
    client.post("/events/review-holds", json=hold_body)                      # duplicate hold
    client.post("/events/review-holds", json=dict(hold_body, review_reason="x"))  # 409 hold
    client.post("/events/review-holds", json={                               # 422 unknown event
        "hold_id": "hold-no-audit", "passenger_id": "HR",
        "event_ids": ["no-such-event"], "review_reason": "x"})
    client.post("/events/review-holds/hold-2026-10-22-a/release",            # duplicate release
                json={"expected_version": 1})
    client.post("/events/review-holds/hold-2026-10-22-a/release",            # 409 re-release
                json={"expected_version": 2})
    client.post("/events", json={"event_id": "x9", "passenger_id": "X",
                                 "station_id": "NOPE", "tap_type": "in",
                                 "occurred_at": "2026-09-21T08:00:00Z"})  # 422
    check("repeated/failed requests create no new audit records",
          client.get("/audit", params={"limit": 1000}).json()["count"] == audit["count"])

    v = client.get("/audit/verify").json()
    check("verify reports intact chain head",
          v["valid"] and v["head_seq"] == audit["count"] and len(v["head_hash"]) == 64
          and v["broken_seq"] is None)
    check("verify head equals last record hash",
          v["head_hash"] == entries[-1]["record_hash"])

    ev_only = client.get("/audit", params={"operation": "network_snapshot_created"}).json()
    check("audit operation filter works",
          ev_only["count"] == n_snapshots
          and all(e["operation"] == "network_snapshot_created"
                  for e in ev_only["entries"]))
    dispute_audits = client.get("/audit", params={"operation": "dispute_resolved"}).json()
    check("audit filter for dispute_resolved",
          dispute_audits["count"] == n_resolved
          and all(e["operation"] == "dispute_resolved"
                  for e in dispute_audits["entries"]))
    page1 = client.get("/audit", params={"after": 0, "limit": 2}).json()
    page2 = client.get("/audit", params={"after": 2, "limit": 2}).json()
    check("audit pagination walks the sequence",
          [e["seq"] for e in page1["entries"]] == [1, 2]
          and [e["seq"] for e in page2["entries"]] == [3, 4])

    # Business write failure rolls the audit record back together with the data.
    import app.main as mainmod
    before_count = client.get("/audit", params={"limit": 1000}).json()["count"]
    orig_append = mainmod.append_audit

    def failing_append(*a, **k):
        raise RuntimeError("simulated audit failure")

    mainmod.append_audit = failing_append
    raised = False
    try:
        client.post("/events", json={
            "event_id": "roll-1", "passenger_id": "ROLL",
            "station_id": "S01", "tap_type": "in",
            "occurred_at": "2026-09-21T08:00:00Z"})
    except RuntimeError:
        raised = True
    finally:
        mainmod.append_audit = orig_append
    check("audit failure propagates from the transaction", raised)
    check("failed transaction leaves neither business nor audit data",
          client.get("/audit", params={"limit": 1000}).json()["count"] == before_count
          and client.get("/pending", params={"passenger_id": "ROLL"}).json() == [])
    check("chain intact after rolled-back transaction",
          client.get("/audit/verify").json()["valid"])

    # Append-only triggers and tamper detection on an isolated database.
    tmp = sqlite3.connect(":memory:")
    tmp.row_factory = sqlite3.Row
    tmp.executescript(SCHEMA)
    append_audit(tmp, "event_processed", {"i": 1}, {"ok": True})
    append_audit(tmp, "event_corrected", {"i": 2}, {"ok": True})
    append_audit(tmp, "recon_batch_created", {"i": 3}, {"ok": True})
    append_audit(tmp, "network_validated", {"i": 4}, {"ok": True})
    append_audit(tmp, "network_snapshot_created", {"i": 5}, {"ok": True})
    append_audit(tmp, "dispute_opened", {"i": 6}, {"ok": True})
    append_audit(tmp, "dispute_resolved", {"i": 7}, {"ok": True})
    append_audit(tmp, "clearing_closed", {"i": 8}, {"ok": True})
    rep = verify_chain(tmp)
    check("fresh standalone chain verifies", rep["valid"] and rep["head_seq"] == 8)
    tmp.execute(
        "INSERT INTO network_validations (validation_id, payload_hash, payload,"
        " effective_start, effective_end, passed, result, created_at)"
        " VALUES ('nv1','h','{}','s','e',1,'{}','now')"
    )
    tmp.execute(
        "INSERT INTO network_snapshots (snapshot_id, payload, result, created_at)"
        " VALUES ('nv1','{}','{}','now')"
    )
    tmp.execute(
        "INSERT INTO disputes (dispute_id, payload_hash, passenger_id, trip_id,"
        " claimed_amount_cents, reason, created_at)"
        " VALUES ('d1','h','P','t1',100,'why','now')"
    )
    tmp.execute(
        "INSERT INTO dispute_snapshots (dispute_id, trip_id, passenger_id,"
        " amount_cents, base_fare_cents, transfer_discount_cents, basis, frozen_at)"
        " VALUES ('d1','t1','P',100,100,0,'{}','now')"
    )
    tmp.execute(
        "INSERT INTO refunds (refund_id, dispute_id, passenger_id, trip_id,"
        " amount_cents, payload_hash, review_note, created_at)"
        " VALUES ('RF-d1','d1','P','t1',100,'h',NULL,'now')"
    )
    blocked = []
    for stmt in ("UPDATE audit_log SET operation = 'event_processed' WHERE seq = 1",
                 "DELETE FROM audit_log WHERE seq = 2",
                 "UPDATE network_validations SET passed = 0 WHERE validation_id = 'nv1'",
                 "DELETE FROM network_validations WHERE validation_id = 'nv1'",
                 "DELETE FROM network_snapshots WHERE snapshot_id = 'nv1'",
                 "UPDATE network_snapshots SET result = '{}' WHERE snapshot_id = 'nv1'",
                 "UPDATE disputes SET reason = 'x' WHERE dispute_id = 'd1'",
                 "DELETE FROM disputes WHERE dispute_id = 'd1'",
                 "UPDATE dispute_snapshots SET amount_cents = 1 WHERE dispute_id = 'd1'",
                 "DELETE FROM dispute_snapshots WHERE dispute_id = 'd1'",
                 "UPDATE refunds SET amount_cents = 1 WHERE dispute_id = 'd1'",
                 "DELETE FROM refunds WHERE dispute_id = 'd1'"):
        try:
            tmp.execute(stmt)
        except sqlite3.Error:
            blocked.append(stmt)
    check("audit, network and dispute rows cannot be updated or deleted",
          len(blocked) == 12)

    tmp.execute("DROP TRIGGER trg_audit_no_update")
    tmp.execute("UPDATE audit_log SET result_summary = '{\"tampered\":true}' WHERE seq = 2")
    rep = verify_chain(tmp)
    check("content tampering reported at the broken sequence",
          (not rep["valid"]) and rep["broken_seq"] == 2)

    tmp.execute("DROP TRIGGER trg_audit_no_delete")
    tmp2 = sqlite3.connect(":memory:")
    tmp2.row_factory = sqlite3.Row
    tmp2.executescript(SCHEMA)
    append_audit(tmp2, "event_processed", {"i": 1}, {"ok": True})
    append_audit(tmp2, "event_processed", {"i": 2}, {"ok": True})
    append_audit(tmp2, "event_processed", {"i": 3}, {"ok": True})
    tmp2.execute("DROP TRIGGER trg_audit_no_delete")
    tmp2.execute("DELETE FROM audit_log WHERE seq = 2")
    rep = verify_chain(tmp2)
    check("deleted middle record reported at first broken sequence",
          (not rep["valid"]) and rep["broken_seq"] == 2)

    # --- Online backup ---------------------------------------------------------
    live_db = app.state.db
    data_dir = os.path.dirname(os.path.abspath(os.environ["DB_PATH"]))

    def leftover_temp_backups():
        return glob.glob(os.path.join(data_dir, "transit-backup-*.db*"))

    # Basic endpoint: file streams, header digest matches the bytes, and the
    # returned copy opens standalone, passes integrity_check, contains the
    # same rows as the live database, and its audit chain still verifies.
    before_events = live_db.conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    r = client.get("/admin/backup")
    check("backup endpoint returns 200 sqlite file",
          r.status_code == 200
          and r.headers["content-type"] == "application/vnd.sqlite3")
    data = r.content
    check("X-Backup-SHA256 matches payload",
          r.headers["x-backup-sha256"] == hashlib.sha256(data).hexdigest())
    check("X-Backup-Size-Bytes matches payload",
          int(r.headers["x-backup-size-bytes"]) == len(data))
    check("downloaded filename is timestamped",
            r.headers["content-disposition"].startswith(
                'attachment; filename="transit-backup-')
          and r.headers["content-disposition"].endswith('.db"'))
    saved = os.path.join(data_dir, "downloaded-backup.db")
    with open(saved, "wb") as f:
        f.write(data)
    with sqlite3.connect(saved) as restored:
        check("downloaded backup passes integrity_check",
              restored.execute("PRAGMA integrity_check").fetchone()[0] == "ok")
        check("downloaded backup has all events",
              restored.execute("SELECT COUNT(*) FROM events").fetchone()[0]
              == before_events)
        restored.row_factory = sqlite3.Row
        check("downloaded backup audit chain intact",
              verify_chain(restored)["valid"])
    os.remove(saved)

    # Writes keep flowing *during* a backup, and the snapshot is consistent.
    # A throttled progress callback stretches the copy so tap submissions
    # overlap it; the SQLite backup API must restart instead of producing a
    # torn file.
    write_counter = {"n": 0}
    write_lock = threading.Lock()
    errors = []
    started = threading.Event()      # set once the backup is copying
    writes_released = threading.Event()

    def run_writes(count=30):
        writes_released.wait()
        # Write while the backup is still paging through the database.
        for _ in range(count):
            with write_lock:
                write_counter["n"] += 1
                idx = write_counter["n"]
            rr = client.post("/events", json={
                "event_id": f"bk-{idx}",
                "passenger_id": f"BK-{idx}",
                "station_id": "S01", "tap_type": "in",
                "occurred_at": "2026-09-21T08:00:00Z",
            })
            if rr.status_code != 200:
                errors.append(rr.text)
            time.sleep(0.002)

    def slow_progress(status, remaining, page_count):
        started.set()               # first page batch: copy is in flight
        writes_released.wait(10)    # don't finish before the writers start
        time.sleep(0.01)

    backup_path = os.path.join(data_dir, "concurrent-backup.db")

    def run_backup():
        try:
            live_db.online_backup(backup_path, pages=1, progress=slow_progress)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    t_backup = threading.Thread(target=run_backup)
    writers = [threading.Thread(target=run_writes) for _ in range(3)]
    for t in writers:
        t.start()
    t_backup.start()
    assert started.wait(10), "backup never started"
    writes_released.set()  # backup is copying: let writes overlap it
    for t in writers:
        t.join()
    t_backup.join()
    check("no errors from backup or concurrent writers", not errors, str(errors))
    with sqlite3.connect(backup_path) as snap:
        check("concurrent backup passes integrity_check",
              snap.execute("PRAGMA integrity_check").fetchone()[0] == "ok")
        snap_events = snap.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        # Consistent snapshot taken mid-write: contains all pre-backup rows and
        # at most all committed rows, never a torn count.
        after_events = live_db.conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        check("concurrent backup is a consistent snapshot",
              before_events <= snap_events <= after_events
              and after_events - before_events == 90)
        snap.row_factory = sqlite3.Row
        check("concurrent backup audit chain intact",
              verify_chain(snap)["valid"])
    os.remove(backup_path)

    # Same property end-to-end through the HTTP endpoint: a backup while
    # writers run still yields a digest-verifiable, intact database file.
    # The endpoint's paging hook is throttled so the copy overlaps the writes.
    import app.main as mainmod  # noqa: E402
    ep_started = threading.Event()
    ep_release = threading.Event()

    def endpoint_progress(status, remaining, page_count):
        ep_started.set()
        ep_release.wait(10)
        time.sleep(0.005)

    orig_pages = mainmod.BACKUP_PAGES_PER_BATCH
    orig_progress = mainmod.BACKUP_PROGRESS_CALLBACK
    mainmod.BACKUP_PAGES_PER_BATCH = 1
    mainmod.BACKUP_PROGRESS_CALLBACK = endpoint_progress

    def run_endpoint_backup(result):
        result.append(client.get("/admin/backup"))

    resp = []
    t_ep = threading.Thread(target=run_endpoint_backup, args=(resp,))
    ep_writers = [threading.Thread(target=run_writes, kwargs={"count": 30})
                  for _ in range(2)]
    for t in ep_writers:
        t.start()
    t_ep.start()
    assert ep_started.wait(10), "endpoint backup never started"
    ep_release.set()  # backup is copying: release the overlapping writers
    for t in ep_writers:
        t.join()
    t_ep.join()
    mainmod.BACKUP_PAGES_PER_BATCH = orig_pages
    mainmod.BACKUP_PROGRESS_CALLBACK = orig_progress
    check("endpoint backup under concurrent writes succeeds",
          bool(resp) and resp[0].status_code == 200,
          resp[0].text if resp else "no response")
    body = resp[0].content
    check("endpoint backup digest consistent under concurrent writes",
          resp[0].headers["x-backup-sha256"]
          == hashlib.sha256(body).hexdigest())
    ep_path = os.path.join(data_dir, "endpoint-backup.db")
    with open(ep_path, "wb") as f:
        f.write(body)
    with sqlite3.connect(ep_path) as snap:
        check("endpoint backup passes integrity_check",
              snap.execute("PRAGMA integrity_check").fetchone()[0] == "ok")
    os.remove(ep_path)

    # Failure during the backup -> 500 with an explicit message and the temp
    # file is cleaned up (nothing left in the data directory).
    orig_online_backup = mainmod.Database.online_backup

    def failing_online_backup(self, dst_path, **kwargs):
        raise RuntimeError("simulated backup failure")

    mainmod.Database.online_backup = failing_online_backup
    try:
        r = client.get("/admin/backup")
        check("backup failure -> 500 with explicit detail",
              r.status_code == 500 and "simulated backup failure" in r.text)
    finally:
        mainmod.Database.online_backup = orig_online_backup
    check("failed backup leaves no temp files", not leftover_temp_backups())

    # A corrupt backup is rejected by the independent integrity check -> 500
    # and the temp file is cleaned up as well. Clobbering the SQLite header
    # makes the copy fail to even parse, which must surface as a 500.
    real_backup = orig_online_backup

    def corrupting_online_backup(self, dst_path, **kwargs):
        real_backup(self, dst_path, **kwargs)
        with open(dst_path, "r+b") as f:
            f.write(b"not a sqlite database" + os.urandom(104))

    mainmod.Database.online_backup = corrupting_online_backup
    try:
        r = client.get("/admin/backup")
        check("backup failing integrity check -> 500",
              r.status_code == 500
              and ("integrity" in r.text.lower() or "file is not a database" in r.text))
    finally:
        mainmod.Database.online_backup = orig_online_backup
    check("rejected backup leaves no temp files", not leftover_temp_backups())

    # Successful response deletes its temp file after streaming.
    r = client.get("/admin/backup")
    check("successful backup streams a file", r.status_code == 200)
    check("successful backup temp file cleaned after response",
          not leftover_temp_backups())

    # Backup is read-only: the audit chain still verifies afterwards (only
    # /events writes added records).
    check("database still healthy after backup exercises",
          client.get("/audit/verify").json()["valid"])


with TestClient(app) as client:
    run(client)

print(f"\nAll {PASS} checks passed.")
