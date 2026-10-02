"""Fare configuration loading and validation."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field


class ConfigError(Exception):
    pass


@dataclass(frozen=True)
class FareConfig:
    currency: str
    transfer_window_minutes: int
    transfer_discount_cents: int
    daily_cap_cents: int
    weekly_cap_cents: int
    stations: dict[str, int] = field(default_factory=dict)
    zone_fares: dict[str, int] = field(default_factory=dict)  # "a-b" with a <= b

    def zone_of(self, station_id: str) -> int | None:
        return self.stations.get(station_id)

    def fare_between(self, zone_a: int, zone_b: int) -> int:
        lo, hi = sorted((zone_a, zone_b))
        return self.zone_fares[f"{lo}-{hi}"]


def load_config(path: str | None = None) -> FareConfig:
    path = path or os.environ.get("FARE_CONFIG", "config/fares.json")
    with open(path, "r", encoding="utf-8") as fh:
        raw = json.load(fh)

    cfg = FareConfig(
        currency=raw.get("currency", "CNY"),
        transfer_window_minutes=int(raw["transfer_window_minutes"]),
        transfer_discount_cents=int(raw["transfer_discount_cents"]),
        daily_cap_cents=int(raw["daily_cap_cents"]),
        weekly_cap_cents=int(raw["weekly_cap_cents"]),
        stations={str(k): int(v) for k, v in raw["stations"].items()},
        zone_fares={str(k): int(v) for k, v in raw["zone_fares"].items()},
    )
    _validate(cfg)
    return cfg


def _validate(cfg: FareConfig) -> None:
    if not cfg.stations:
        raise ConfigError("config must define at least one station")
    zones = sorted(set(cfg.stations.values()))
    missing = [
        f"{a}-{b}"
        for i, a in enumerate(zones)
        for b in zones[i:]
        if f"{a}-{b}" not in cfg.zone_fares
    ]
    if missing:
        raise ConfigError(f"zone_fares missing entries for zone pairs: {missing}")
    for name in ("daily_cap_cents", "weekly_cap_cents", "transfer_discount_cents"):
        if getattr(cfg, name) < 0:
            raise ConfigError(f"{name} must be >= 0")
    if cfg.transfer_window_minutes < 0:
        raise ConfigError("transfer_window_minutes must be >= 0")
