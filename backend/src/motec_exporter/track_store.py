from __future__ import annotations

import datetime as dt
import json
import re
from pathlib import Path

from .models import TrackDefinition


SLUG_RE = re.compile(r"[^a-z0-9]+")


def slugify(value: str) -> str:
    slug = SLUG_RE.sub("-", value.lower()).strip("-")
    return slug or "track"


class TrackStore:
    def __init__(self, root: Path):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def list_tracks(self) -> list[TrackDefinition]:
        tracks: list[TrackDefinition] = []
        for path in sorted(self.root.glob("*.json")):
            tracks.append(self.load(path.stem))
        return tracks

    def load(self, slug: str) -> TrackDefinition:
        path = self._path(slug)
        data = json.loads(path.read_text(encoding="utf-8"))
        return TrackDefinition.model_validate(data)

    def save(self, track: TrackDefinition) -> TrackDefinition:
        now = dt.datetime.now(dt.UTC).isoformat()
        slug = slugify(track.slug or track.name)
        path = self._path(slug)
        created_at = track.created_at
        if not created_at and path.exists():
            try:
                created_at = self.load(slug).created_at
            except Exception:
                created_at = now
        track = track.model_copy(update={"slug": slug, "created_at": created_at or now, "updated_at": now})
        path.write_text(json.dumps(track.model_dump(), indent=2) + "\n", encoding="utf-8")
        return track

    def _path(self, slug: str) -> Path:
        safe = slugify(slug)
        return self.root / f"{safe}.json"

