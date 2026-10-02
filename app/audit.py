"""Append-only, hash-chained business audit log.

Every successful business write (a tap event processed/corrected, a
reconciliation batch created) appends one audit record inside the same
SQLite transaction as the business data. Records form a single global
chain:

    record_hash_0 = sha256(seq | operation | occurred_at
                           | request_summary | result_summary | "")
    record_hash_n = sha256(seq | operation | occurred_at
                           | request_summary | result_summary
                           | record_hash_{n-1})

Summaries are canonical JSON (``sort_keys``, compact separators, UTC
timestamps), so a verifier can reconstruct every hash purely from the
stored rows. The table itself carries UPDATE/DELETE triggers, making the
log append-only at the storage layer as well.
"""
from __future__ import annotations

import hashlib
import hmac
import json
from datetime import datetime, timezone
from typing import Any

GENESIS_HASH = ""


def canonical_json(obj: Any) -> str:
    """Deterministic JSON encoding used both for stored summaries and hashing."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def compute_hash(
    seq: int,
    operation: str,
    occurred_at: str,
    request_summary: str,
    result_summary: str,
    prev_hash: str,
) -> str:
    payload = canonical_json(
        {
            "seq": seq,
            "operation": operation,
            "occurred_at": occurred_at,
            "request_summary": request_summary,
            "result_summary": result_summary,
            "prev_hash": prev_hash,
        }
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def append_audit(
    conn,
    operation: str,
    request: Any,
    result: Any,
    occurred_at: datetime | None = None,
) -> dict:
    """Append one audit record, chaining it to the current chain head.

    Must be called inside the caller's open transaction; raises if no
    sequence slot is available (e.g. another uncommitted writer, which the
    application lock rules out). Returns the stored record as a dict.
    """
    occurred_s = (
        occurred_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        if occurred_at is not None
        else datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    )
    request_summary = canonical_json(request)
    result_summary = canonical_json(result)

    head = conn.execute(
        "SELECT seq, record_hash FROM audit_log ORDER BY seq DESC LIMIT 1"
    ).fetchone()
    if head is None:
        seq, prev_hash = 1, GENESIS_HASH
    else:
        seq, prev_hash = head["seq"] + 1, head["record_hash"]

    record_hash = compute_hash(
        seq, operation, occurred_s, request_summary, result_summary, prev_hash
    )
    conn.execute(
        "INSERT INTO audit_log (seq, operation, occurred_at, request_summary,"
        " result_summary, prev_hash, record_hash)"
        " VALUES (?,?,?,?,?,?,?)",
        (
            seq,
            operation,
            occurred_s,
            request_summary,
            result_summary,
            prev_hash,
            record_hash,
        ),
    )
    return {
        "seq": seq,
        "operation": operation,
        "occurred_at": occurred_s,
        "request_summary": json.loads(request_summary),
        "result_summary": json.loads(result_summary),
        "prev_hash": prev_hash,
        "record_hash": record_hash,
    }


def verify_chain(conn) -> dict:
    """Walk the whole chain and return the head, or the first break found.

    A break is reported at the first record whose position is wrong
    (sequence gap or duplicate), whose ``prev_hash`` does not match the
    previous record, or whose content does not hash to its stored
    ``record_hash``. An empty chain is considered intact with no head.
    """
    rows = conn.execute(
        "SELECT seq, operation, occurred_at, request_summary, result_summary,"
        " prev_hash, record_hash FROM audit_log ORDER BY seq"
    ).fetchall()

    expected_seq = 1
    prev_hash = GENESIS_HASH
    for r in rows:
        expected_hash = compute_hash(
            r["seq"],
            r["operation"],
            r["occurred_at"],
            r["request_summary"],
            r["result_summary"],
            r["prev_hash"],
        )
        if r["seq"] != expected_seq or r["prev_hash"] != prev_hash:
            return {
                "valid": False,
                "head_seq": None,
                "head_hash": None,
                "broken_seq": expected_seq,
                "reason": (
                    f"chain linkage broken at sequence {expected_seq}: expected a"
                    f" record with seq={expected_seq} chained to {prev_hash!r}"
                ),
                "count": len(rows),
            }
        if not hmac.compare_digest(expected_hash, r["record_hash"]):
            return {
                "valid": False,
                "head_seq": None,
                "head_hash": None,
                "broken_seq": r["seq"],
                "reason": (
                    f"record {r['seq']} content or hash has been tampered with:"
                    f" stored record_hash does not match the recomputed hash"
                ),
                "count": len(rows),
            }
        prev_hash = r["record_hash"]
        expected_seq += 1

    head_seq = rows[-1]["seq"] if rows else 0
    return {
        "valid": True,
        "head_seq": head_seq,
        "head_hash": prev_hash if rows else None,
        "broken_seq": None,
        "reason": "chain intact" if rows else "empty audit chain",
        "count": len(rows),
    }
