"""Pydantic request/response schemas."""
from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, field_validator


class TapEventIn(BaseModel):
    event_id: str = Field(min_length=1, max_length=128)
    passenger_id: str = Field(min_length=1, max_length=128)
    station_id: str = Field(min_length=1, max_length=64)
    tap_type: Literal["in", "out"]
    occurred_at: datetime  # any ISO-8601 offset accepted, normalised to UTC


class Adjustment(BaseModel):
    trip_id: str
    day_key: str | None
    week_key: str | None
    old_amount_cents: int
    new_amount_cents: int
    delta_cents: int
    basis: dict


class EventResult(BaseModel):
    event_id: str
    status: Literal["processed", "duplicate", "corrected"]
    event_pairing: Literal["paired", "pending"]
    pending_reason: str | None = None
    adjustments: list[Adjustment]


class PendingEvent(BaseModel):
    event_id: str
    passenger_id: str
    station_id: str
    tap_type: str
    occurred_at: str
    reason: str


class TripOut(BaseModel):
    trip_id: str
    from_station: str
    to_station: str
    from_zone: int
    to_zone: int
    started_at: str
    ended_at: str
    day_key: str
    week_key: str
    base_fare_cents: int
    transfer_discount_cents: int
    amount_cents: int
    basis: dict


class PeriodTotal(BaseModel):
    key: str
    amount_cents: int
    cap_cents: int
    trip_count: int


class Bill(BaseModel):
    passenger_id: str
    currency: str
    trips: list[TripOut]
    daily_totals: list[PeriodTotal]
    weekly_totals: list[PeriodTotal]
    total_cents: int


class ReconRecordIn(BaseModel):
    serial_no: str = Field(min_length=1, max_length=128)
    passenger_id: str = Field(min_length=1, max_length=128)
    charged_at: datetime  # any ISO-8601 offset accepted, normalised to UTC
    amount_cents: int = Field(ge=0)
    trip_id: str | None = Field(default=None, min_length=1, max_length=256)


class ReconBatchIn(BaseModel):
    batch_id: str = Field(min_length=1, max_length=128)
    window_start: datetime  # half-open UTC window [window_start, window_end)
    window_end: datetime    # on the internal trips' tap-out time (ended_at)
    tolerance_seconds: int = Field(ge=0)
    records: list[ReconRecordIn] = Field(default_factory=list)


class ReconResultItem(BaseModel):
    category: Literal[
        "matched", "overcharged", "undercharged",
        "internal_only", "external_only", "ambiguous",
    ]
    passenger_id: str
    trip_id: str | None = None
    serial_no: str | None = None
    internal_amount_cents: int | None = None
    external_amount_cents: int | None = None
    difference_cents: int | None = None  # external - internal
    internal_ended_at: str | None = None
    external_charged_at: str | None = None
    match_method: Literal["trip_id", "tolerance"] | None = None
    reason: str


class ReconSummary(BaseModel):
    matched: int
    overcharged: int
    undercharged: int
    internal_only: int
    external_only: int
    ambiguous: int
    snapshot_trip_count: int
    external_record_count: int


class ReconBatchOut(BaseModel):
    batch_id: str
    status: Literal["created", "duplicate"]
    window_start: str
    window_end: str
    tolerance_seconds: int
    currency: str
    summary: ReconSummary
    results: list[ReconResultItem]


AuditOperation = Literal[
    "event_processed", "event_corrected", "recon_batch_created",
    "network_validated", "network_snapshot_created",
    "dispute_opened", "dispute_resolved", "clearing_closed",
    "event_held", "event_hold_released",
]


class AuditEntry(BaseModel):
    seq: int
    operation: AuditOperation
    occurred_at: str
    request_summary: dict
    result_summary: dict
    prev_hash: str
    record_hash: str


class AuditList(BaseModel):
    count: int
    entries: list[AuditEntry]


class AuditVerification(BaseModel):
    valid: bool
    head_seq: int | None
    head_hash: str | None
    broken_seq: int | None
    reason: str
    count: int


# ---------------------------------------------------------------------------
# Transfer network validation
# ---------------------------------------------------------------------------


class NetworkSegmentIn(BaseModel):
    line_id: str = Field(min_length=1, max_length=64)
    segment_id: str = Field(min_length=1, max_length=64)
    from_station: str = Field(min_length=1, max_length=64)
    to_station: str = Field(min_length=1, max_length=64)
    start_at: datetime   # half-open active interval [start_at, end_at), UTC-normalised
    end_at: datetime


class NetworkTransferIn(BaseModel):
    from_station: str = Field(min_length=1, max_length=64)
    to_station: str = Field(min_length=1, max_length=64)
    start_at: datetime
    end_at: datetime


class NetworkValidateIn(BaseModel):
    validation_id: str = Field(min_length=1, max_length=128)
    effective_start: datetime  # half-open UTC window [effective_start, effective_end)
    effective_end: datetime
    hubs: list[str] = Field(min_length=1, max_length=64)
    stations: list[str] = Field(min_length=1, max_length=64)
    segments: list[NetworkSegmentIn] = Field(default_factory=list)
    transfers: list[NetworkTransferIn] = Field(default_factory=list)


class NetworkSlice(BaseModel):
    index: int
    start: str
    end: str


class NetworkHop(BaseModel):
    from_station: str
    to_station: str
    edge_kind: Literal["segment", "transfer"]
    line_id: str | None = None
    segment_id: str | None = None


class NetworkConflict(BaseModel):
    code: Literal[
        "self_loop_edge",
        "overlapping_interval",
        "duplicate_edge",
        "transfer_asymmetry",
        "unreachable_station",
    ]
    slice: NetworkSlice
    message: str
    edge: dict | None = None
    edges: list[dict] | None = None
    hub: str | None = None
    station: str | None = None
    evidence_path: list[NetworkHop] | None = None


class NetworkValidateOut(BaseModel):
    validation_id: str
    status: Literal["created", "duplicate"]
    passed: bool
    effective_start: str
    effective_end: str
    slice_count: int
    slices: list[NetworkSlice]
    conflict: NetworkConflict | None


class NetworkSnapshotIn(BaseModel):
    validation_id: str = Field(min_length=1, max_length=128)


class NetworkSnapshotOut(BaseModel):
    snapshot_id: str
    status: Literal["created", "duplicate"]
    effective_start: str
    effective_end: str
    frozen_at: str
    validation: NetworkValidateOut


# ---------------------------------------------------------------------------
# Fare disputes and refund settlement
# ---------------------------------------------------------------------------


class DisputeIn(BaseModel):
    dispute_id: str = Field(min_length=1, max_length=128)
    passenger_id: str = Field(min_length=1, max_length=128)
    trip_id: str = Field(min_length=1, max_length=256)
    claimed_amount_cents: int = Field(ge=0)  # requested refund, <= snapshot receivable
    reason: str = Field(min_length=1, max_length=2000)


class DisputeResolveIn(BaseModel):
    expected_version: int = Field(ge=1)  # optimistic version the review was based on
    approved_amount_cents: int = Field(ge=0)  # may be 0 (rejected) up to the claim
    review_note: str | None = Field(default=None, max_length=2000)


class ChargeSnapshot(BaseModel):
    trip_id: str
    passenger_id: str
    amount_cents: int               # frozen receivable at dispute creation
    base_fare_cents: int
    transfer_discount_cents: int
    basis: dict
    frozen_at: str


class RefundRecord(BaseModel):
    refund_id: str
    dispute_id: str
    passenger_id: str
    trip_id: str
    amount_cents: int               # immutable, written once on resolution
    review_note: str | None
    created_at: str


class RecomputedSettlement(BaseModel):
    current_amount_cents: int       # live trip charge after event corrections
    # settled_amount_cents = frozen receivable - approved refund (net the
    # passenger actually paid). Present only for resolved disputes.
    settled_amount_cents: int | None = None
    difference_cents: int           # settled_amount - current; positive = clawback, negative = top-up
    direction: Literal["none", "top_up_due", "clawback_due"]


class DisputeOut(BaseModel):
    dispute_id: str
    status: Literal["created", "duplicate"]  # request replay marker
    state: Literal["open", "resolved"]
    version: int                    # 1 while open, 2 once resolved (optimistic)
    passenger_id: str
    trip_id: str
    claimed_amount_cents: int
    reason: str
    created_at: str
    resolved_at: str | None
    snapshot: ChargeSnapshot
    refund: RefundRecord | None
    recomputed: RecomputedSettlement


# ---------------------------------------------------------------------------
# Operator fare clearing (运营方票款清分)
# ---------------------------------------------------------------------------


class ClearingShareIn(BaseModel):
    operator_id: str = Field(min_length=1, max_length=128)
    basis_points: int = Field(
        ge=0,
        le=10000,
        description="Share in basis points (万分比); the pair's shares must sum to 10000",
    )


class ClearingRuleIn(BaseModel):
    from_station: str = Field(min_length=1, max_length=64)
    to_station: str = Field(min_length=1, max_length=64)
    shares: list[ClearingShareIn] = Field(
        min_length=1,
        description="Per-operator basis-point shares; must sum to exactly 10000",
    )


class ClearingIn(BaseModel):
    clearing_id: str = Field(min_length=1, max_length=128)
    window_start: datetime  # half-open UTC window [window_start, window_end)
    window_end: datetime    # on the trips' tap-out time (ended_at)
    rules: list[ClearingRuleIn] = Field(min_length=1)


class ClearingAdjustmentIn(BaseModel):
    clearing_id: str = Field(min_length=1, max_length=128)
    base_clearing_id: str = Field(
        min_length=1,
        max_length=128,
        description="The first closing (seq 0) whose frozen window and rules apply",
    )


class ClearingRuleOut(BaseModel):
    from_station: str
    to_station: str
    shares: list[ClearingShareIn]


class ClearingEntryOut(BaseModel):
    seq: int
    trip_id: str
    passenger_id: str
    from_station: str
    to_station: str
    ended_at: str
    operator_id: str
    basis_points: int | None      # null on adjustment delta rows
    gross_amount_cents: int       # current trip gross for this batch's view (0 once reversed)
    amount_cents: int             # initial split, or signed delta on adjustment rows
    prior_amount_cents: int       # cumulative per trip/operator booked before this batch
    cumulative_amount_cents: int  # cumulative per trip/operator after this batch
    kind: Literal["initial", "adjustment"]


class ClearingOperatorTotal(BaseModel):
    operator_id: str
    amount_cents: int             # cumulative booked position across the whole chain


class ClearingBatchOut(BaseModel):
    clearing_id: str
    root_clearing_id: str
    status: Literal["created", "duplicate"]
    kind: Literal["initial", "adjustment"]
    seq: int                        # 0 for the first closing, 1+ for follow-up batches
    window_start: str
    window_end: str
    rules: list[ClearingRuleOut]
    base_clearing_id: str | None    # the referenced first closing on adjustment batches
    trip_count: int                 # current trips seen by this batch inside the window
    reversed_trip_count: int        # trips reversed to zero (vanished / left the window)
    ledger_row_count: int
    gross_amount_cents: int         # sum of current trips' gross (0-amount deltas omitted)
    net_amount_cents: int           # signed amount this batch books (initial gross; delta sum later)
    entries: list[ClearingEntryOut]
    operator_totals: list[ClearingOperatorTotal]  # cumulative chain-wide positions
    created_at: str


# ---------------------------------------------------------------------------
# Tap-event quality scans
# ---------------------------------------------------------------------------


class EventQualityScanIn(BaseModel):
    scan_id: str = Field(min_length=1, max_length=128)
    window_start: datetime  # half-open UTC window [window_start, window_end)
    window_end: datetime    # on the events' occurred_at
    passengers: list[str] | None = Field(
        default=None, min_length=1,
        description="Optional passenger allow-list; absent scans everyone",
    )
    duplicate_window_seconds: int = Field(
        ge=0,
        description="Adjacent same-type taps within this window are duplicates",
    )
    min_travel_seconds: dict[str, dict[str, int]] = Field(
        default_factory=dict,
        description=(
            "Directed station-pair minimum travel seconds, keyed "
            "{tap_out_station: {tap_in_station: seconds}}"
        ),
    )


class QualityEvidenceEvent(BaseModel):
    event_id: str
    passenger_id: str
    station_id: str
    tap_type: Literal["in", "out"]
    occurred_at: str
    seq: int


class QualityAlert(BaseModel):
    event_id: str                   # the triggering (later) event
    passenger_id: str
    rule: Literal["duplicate_tap", "min_travel_violation"]
    actual_interval_seconds: float  # elapsed gap between the evidence events
    threshold_seconds: int          # configured bound that was breached
    evidence: list[QualityEvidenceEvent]  # ordered chain, oldest first


class EventQualityScanOut(BaseModel):
    scan_id: str
    status: Literal["created", "duplicate"]
    window_start: str
    window_end: str
    passengers: list[str] | None    # null = all passengers scanned
    duplicate_window_seconds: int
    min_travel_seconds: dict[str, dict[str, int]]
    event_count: int
    alert_count: int
    alerts: list[QualityAlert]


# ---------------------------------------------------------------------------
# Anonymous k-anonymous trajectory exports
# ---------------------------------------------------------------------------


class PrivacyExportIn(BaseModel):
    export_id: str = Field(min_length=1, max_length=128)
    window_start: datetime  # half-open UTC window [window_start, window_end)
    window_end: datetime    # on the trips' tap-in time (started_at)
    bucket_minutes: int = Field(
        ge=1,
        description="Fixed-width UTC time bucket in minutes, epoch-anchored",
    )
    station_groups: dict[str, str] = Field(
        min_length=1,
        description=(
            "Station id -> group label; the same mapping applies to the trip's"
            " origin (from_station) and destination (to_station)"
        ),
    )
    k: int = Field(
        ge=1,
        description=(
            "Minimum distinct passengers for an equivalence class"
            " (bucket, origin group, destination group) to be retained"
        ),
    )

    @field_validator("station_groups")
    @classmethod
    def _non_empty_mapping(cls, value: dict[str, str]) -> dict[str, str]:
        empty_stations = [station for station, group in value.items()
                          if not station or not group]
        if empty_stations:
            raise ValueError(
                "station_groups keys and values must be non-empty strings"
            )
        return value


class PrivacyExportTrip(BaseModel):
    pseudonym: str          # HMAC-SHA256 hex of passenger_id under PRIVACY_SALT
    bucket_start: str       # start of the epoch-anchored UTC bucket, ISO-8601
    origin_group: str
    destination_group: str


class PrivacyEquivalenceClass(BaseModel):
    bucket_start: str
    origin_group: str
    destination_group: str
    passenger_count: int    # distinct pseudonymised passengers in this class
    trip_count: int         # retained trips in this class


class PrivacyExportOut(BaseModel):
    export_id: str
    status: Literal["created", "duplicate"]
    window_start: str
    window_end: str
    bucket_minutes: int
    k: int
    station_groups: dict[str, str]
    input_trip_count: int
    equivalence_class_count: int
    retained_class_count: int
    suppressed_class_count: int
    retained_trip_count: int
    suppressed_trip_count: int
    classes: list[PrivacyEquivalenceClass]
    trips: list[PrivacyExportTrip]


# ---------------------------------------------------------------------------
# Sudden-disruption evacuation replanning
# ---------------------------------------------------------------------------


class ReplanStopIn(BaseModel):
    station_id: str = Field(min_length=1, max_length=64)
    arrival_at: datetime    # any ISO-8601 offset accepted, normalised to UTC
    departure_at: datetime


class ReplanServiceIn(BaseModel):
    service_id: str = Field(min_length=1, max_length=128)
    capacity: int = Field(
        ge=0,
        description="Rated capacity of the replacement service on every section",
    )
    stops: list[ReplanStopIn] = Field(
        min_length=2,
        description="Stops in station order; at least two form one section",
    )


class ReplanGroupIn(BaseModel):
    group_id: str = Field(min_length=1, max_length=128)
    origin: str = Field(min_length=1, max_length=64)
    destination: str = Field(min_length=1, max_length=64)
    ready_at: datetime      # earliest UTC time the whole party can board
    party_size: int = Field(ge=1)  # indivisible party


class DisruptionReplanIn(BaseModel):
    replan_id: str = Field(min_length=1, max_length=128)
    min_transfer_seconds: int = Field(
        ge=0,
        description="Minimum gap between arriving on one service and departing on the next",
    )
    services: list[ReplanServiceIn] = Field(default_factory=list)
    groups: list[ReplanGroupIn] = Field(default_factory=list)


class ReplanLeg(BaseModel):
    service_id: str
    from_station: str
    to_station: str
    departure_at: str       # departure from from_station, UTC
    arrival_at: str         # arrival at to_station, UTC


class ReplanGroupResult(BaseModel):
    group_id: str
    origin: str
    destination: str
    ready_at: str
    party_size: int
    status: Literal["assigned", "stranded"]
    arrival_at: str | None = None       # null while stranded
    transfers: int | None = None        # legs - 1; null while stranded
    legs: list[ReplanLeg]
    stranded_reason: str | None = None


class ReplanSummary(BaseModel):
    group_count: int
    assigned_count: int
    stranded_count: int


class DisruptionReplanOut(BaseModel):
    replan_id: str
    status: Literal["created", "duplicate"]
    min_transfer_seconds: int
    summary: ReplanSummary
    groups: list[ReplanGroupResult]


# ---------------------------------------------------------------------------
# Tap-event review holds (刷卡事件复核暂存与释放)
# ---------------------------------------------------------------------------


class EventHoldIn(BaseModel):
    hold_id: str = Field(min_length=1, max_length=128)
    passenger_id: str = Field(min_length=1, max_length=128)
    event_ids: list[str] = Field(
        min_length=1, description="Complete payloads of these events are frozen for review",
    )
    review_reason: str = Field(min_length=1, max_length=2000)


class EventHoldReleaseIn(BaseModel):
    expected_version: int = Field(
        ge=1,
        description="Optimistic hold version the release is based on (1 while held, 2 once released)",
    )


class HeldEventSnapshot(BaseModel):
    seq: int                         # 0-based, canonical (sorted event id) order
    event_id: str
    passenger_id: str
    station_id: str
    tap_type: Literal["in", "out"]
    occurred_at: str
    event_seq: int                   # the event's original arrival seq, preserved on release
    payload_hash: str                # sha256 of the canonical single-event payload
    frozen_at: str


class HoldReleaseResult(BaseModel):
    release_id: str
    expected_version: int
    released_at: str
    adjustments: list[Adjustment]    # signed per-trip deltas after re-settlement


class EventHoldOut(BaseModel):
    hold_id: str
    status: Literal["created", "duplicate"]   # request replay marker
    state: Literal["held", "released"]
    version: int                    # 1 while held, 2 once released (optimistic)
    passenger_id: str
    event_ids: list[str]            # canonical (sorted, de-duplicated) order
    review_reason: str
    created_at: str
    released_at: str | None
    snapshots: list[HeldEventSnapshot]
    release: HoldReleaseResult | None = None
    # Settlement deltas produced when the events were removed from pairing and
    # billing at hold time; present on the POST response (and frozen in storage).
    adjustments: list[Adjustment] = []
    trip_change_count: int = 0
