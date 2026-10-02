"""Fare disputes and refund settlement.

A dispute freezes the trip's receivable charge at creation time (a copy of the
``trip_charges`` row), so the review always judges the amount that was due when
the dispute was opened. Resolution is a one-shot, optimistic-concurrency write:
the client submits the version it reviewed (1 while open); on a match the
dispute closes (version 2) and exactly one immutable refund row is written, in
the same transaction as the ``dispute_resolved`` audit record.

Settled refunds are never rewritten. When event corrections later recompute the
trip charge, GET recomputes the live amount on the fly and compares it with the
net amount the passenger actually paid (``frozen receivable - approved
refund``):

* live amount < net paid  -> ``top_up_due`` (the passenger is owed more);
* live amount > net paid  -> ``clawback_due`` (the passenger owes the excess);
* equal                   -> ``none``.

The difference is reported only; no automatic second refund is created, and the
stored refund stays untouched.
"""
from __future__ import annotations

import hashlib
import json

from .audit import canonical_json
from .models import (
    ChargeSnapshot,
    DisputeOut,
    RefundRecord,
    RecomputedSettlement,
)

REFUND_ID_PREFIX = "RF-"


def refund_id_for(dispute_id: str) -> str:
    return f"{REFUND_ID_PREFIX}{dispute_id}"


def canonical_dispute_payload(body) -> dict:
    """Canonical create request: no timestamps are involved, but field order
    is normalised so the stored hash is deterministic."""
    return {
        "dispute_id": body.dispute_id,
        "passenger_id": body.passenger_id,
        "trip_id": body.trip_id,
        "claimed_amount_cents": body.claimed_amount_cents,
        "reason": body.reason,
    }


def canonical_resolve_payload(body, dispute_id: str) -> dict:
    """Canonical resolve request. ``dispute_id`` comes from the path, so it is
    included in the hash to make the ledger entry self-describing."""
    return {
        "dispute_id": dispute_id,
        "expected_version": body.expected_version,
        "approved_amount_cents": body.approved_amount_cents,
        "review_note": body.review_note,
    }


def payload_hash(payload: dict) -> str:
    return hashlib.sha256(
        canonical_json(payload).encode("utf-8")
    ).hexdigest()


def _recomputed(conn, snap_row, refund_row) -> RecomputedSettlement:
    live = conn.execute(
        "SELECT amount_cents FROM trip_charges WHERE trip_id = ?",
        (snap_row["trip_id"],),
    ).fetchone()
    # A trip that no longer exists after re-pairing has a live amount of 0.
    current = live["amount_cents"] if live is not None else 0

    if refund_row is None:
        # Still open: nothing has been paid back yet, so no settlement gap is
        # reported; the reviewer works against the frozen snapshot.
        return RecomputedSettlement(
            current_amount_cents=current,
            settled_amount_cents=None,
            difference_cents=0,
            direction="none",
        )

    settled = snap_row["amount_cents"] - refund_row["amount_cents"]
    # difference > 0: the passenger net-paid more than the recomputed fare
    # (a further refund is due); difference < 0: the passenger paid too little
    # (the excess is to be clawed back). The stored refund is never touched.
    difference = settled - current
    if difference > 0:
        direction = "top_up_due"
    elif difference < 0:
        direction = "clawback_due"
    else:
        direction = "none"
    return RecomputedSettlement(
        current_amount_cents=current,
        settled_amount_cents=settled,
        difference_cents=difference,
        direction=direction,
    )


def dispute_response(conn, dispute_row, status: str) -> DisputeOut:
    """Build the API view for a stored dispute. ``status`` is the replay marker
    ("created" or "duplicate"); the persistent state is derived from whether
    the immutable refund row exists, never from a mutable column."""
    snap = conn.execute(
        "SELECT * FROM dispute_snapshots WHERE dispute_id = ?",
        (dispute_row["dispute_id"],),
    ).fetchone()
    refund = conn.execute(
        "SELECT * FROM refunds WHERE dispute_id = ?",
        (dispute_row["dispute_id"],),
    ).fetchone()

    snapshot = ChargeSnapshot(
        trip_id=snap["trip_id"],
        passenger_id=snap["passenger_id"],
        amount_cents=snap["amount_cents"],
        base_fare_cents=snap["base_fare_cents"],
        transfer_discount_cents=snap["transfer_discount_cents"],
        basis=json.loads(snap["basis"]),
        frozen_at=snap["frozen_at"],
    )
    refund_model = (
        RefundRecord(
            refund_id=refund["refund_id"],
            dispute_id=refund["dispute_id"],
            passenger_id=refund["passenger_id"],
            trip_id=refund["trip_id"],
            amount_cents=refund["amount_cents"],
            review_note=refund["review_note"],
            created_at=refund["created_at"],
        )
        if refund is not None
        else None
    )
    return DisputeOut(
        dispute_id=dispute_row["dispute_id"],
        status=status,
        state="resolved" if refund is not None else "open",
        version=2 if refund is not None else 1,
        passenger_id=dispute_row["passenger_id"],
        trip_id=dispute_row["trip_id"],
        claimed_amount_cents=dispute_row["claimed_amount_cents"],
        reason=dispute_row["reason"],
        created_at=dispute_row["created_at"],
        resolved_at=refund["created_at"] if refund is not None else None,
        snapshot=snapshot,
        refund=refund_model,
        recomputed=_recomputed(conn, snap, refund),
    )
