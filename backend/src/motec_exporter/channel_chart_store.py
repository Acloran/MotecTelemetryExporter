from __future__ import annotations

import datetime as dt
import json
import re
from pathlib import Path

from .datalog import DataLog
from .models import ChannelChartDefinition


SLUG_RE = re.compile(r"[^a-z0-9]+")


def slugify(value: str) -> str:
    slug = SLUG_RE.sub("-", value.lower()).strip("-")
    return slug or "channel-chart"


def normalize_channel_name(value: str) -> str:
    return " ".join(str(value or "").strip().casefold().split())


def apply_channel_chart(data_log: DataLog, chart: ChannelChartDefinition | None) -> int:
    if chart is None:
        return 0
    lookup = {
        normalize_channel_name(entry.channel_name): entry
        for entry in chart.entries
        if entry.channel_name.strip()
    }
    matched = 0
    for channel in data_log.channels.values():
        entry = lookup.get(normalize_channel_name(channel.name))
        if entry is None:
            continue
        if entry.quantity_type:
            channel.quantity = entry.quantity_type
        if entry.unit:
            channel.unit = entry.unit
        matched += 1
    return matched


class ChannelChartStore:
    def __init__(self, root: Path):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def list_charts(self) -> list[ChannelChartDefinition]:
        charts: list[ChannelChartDefinition] = []
        for path in sorted(self.root.glob("*.json")):
            charts.append(self.load(path.stem))
        return charts

    def load(self, slug: str) -> ChannelChartDefinition:
        path = self._path(slug)
        data = json.loads(path.read_text(encoding="utf-8"))
        return ChannelChartDefinition.model_validate(data)

    def save(self, chart: ChannelChartDefinition) -> ChannelChartDefinition:
        now = dt.datetime.now(dt.UTC).isoformat()
        slug = slugify(chart.slug or chart.name)
        path = self._path(slug)
        created_at = chart.created_at
        if not created_at and path.exists():
            try:
                created_at = self.load(slug).created_at
            except Exception:
                created_at = now
        chart = chart.model_copy(update={"slug": slug, "created_at": created_at or now, "updated_at": now})
        path.write_text(json.dumps(chart.model_dump(), indent=2) + "\n", encoding="utf-8")
        return chart

    def _path(self, slug: str) -> Path:
        return self.root / f"{slugify(slug)}.json"
