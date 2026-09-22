"""运行时配置：溢流口清单、泵闸能力、阈值与预警等级。"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import timezone, timedelta
from pathlib import Path

DEFAULT_REFERENCE = Path(__file__).resolve().parents[2] / "reference" / "cso_assets.json"
LOCAL_TZ = timezone(timedelta(hours=8))  # Asia/Shanghai，服务统一使用带时区的时间


@dataclass(frozen=True)
class Config:
    raw: dict

    @property
    def timezone(self) -> timezone:
        return LOCAL_TZ

    @property
    def params(self) -> dict:
        return self.raw["parameters"]

    @property
    def zones(self) -> dict:
        return {z["id"]: z for z in self.raw["zones"]}

    @property
    def stations(self) -> dict:
        return {s["id"]: s for s in self.raw["stations"]}

    @property
    def gates(self) -> dict:
        result = {}
        for zone in self.raw["zones"]:
            for gate in zone.get("gates", []):
                result[gate["id"]] = {**gate, "zone_id": zone["id"]}
        return result

    @property
    def pumps(self) -> dict:
        result = {}
        for zone in self.raw["zones"]:
            for pump in zone.get("pumps", []):
                result[pump["id"]] = {**pump, "zone_id": zone["id"]}
        return result

    @property
    def river_reaches(self) -> dict:
        return {r["id"]: r for r in self.raw["river_reaches"]}

    @property
    def warning_levels(self) -> list:
        return sorted(self.raw["warning_levels"], key=lambda w: w["rain_1h_mm_gte"])

    @property
    def thresholds(self) -> dict:
        return self.raw["thresholds"]

    @property
    def storm_samples(self) -> dict:
        return {s["id"]: s for s in self.raw.get("storm_samples", [])}

    def station(self, station_id: str) -> dict | None:
        return self.raw_station_map().get(station_id)

    def raw_station_map(self) -> dict:
        return {s["id"]: s for s in self.raw["stations"]}


def load_config(path: str | os.PathLike | None = None) -> Config:
    path = Path(path) if path else DEFAULT_REFERENCE
    with path.open("r", encoding="utf-8") as fh:
        return Config(json.load(fh))
