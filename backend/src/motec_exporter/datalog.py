from __future__ import annotations

import math
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Sample:
    t: float
    value: float


@dataclass
class Channel:
    name: str
    unit: str = ""
    quantity: str = ""
    samples: list[Sample] = field(default_factory=list)

    @property
    def start(self) -> float:
        return self.samples[0].t if self.samples else 0.0

    @property
    def end(self) -> float:
        return self.samples[-1].t if self.samples else 0.0

    @property
    def average_frequency(self) -> float:
        if len(self.samples) < 2 or self.end <= self.start:
            return 0.0
        return (len(self.samples) - 1) / (self.end - self.start)

    def extract(self, start: float, end: float, *, rebase: bool = True) -> "Channel":
        offset = start if rebase else 0.0
        return Channel(
            name=self.name,
            unit=self.unit,
            quantity=self.quantity,
            samples=[
                Sample(sample.t - offset, sample.value)
                for sample in self.samples
                if start <= sample.t <= end and math.isfinite(sample.value)
            ],
        )

    def resample(self, start: float, end: float, frequency_hz: float | None) -> None:
        if not self.samples:
            return
        target_hz = frequency_hz or max(1.0, round(self.average_frequency or 20.0))
        if target_hz <= 0 or end <= start:
            return
        step = 1.0 / target_hz
        count = max(1, int(math.floor((end - start) * target_hz)) + 1)
        source = self.samples
        out: list[Sample] = []
        index = 0
        for n in range(count):
            t = min(end, start + n * step)
            if t <= source[0].t:
                value = source[0].value
            elif t >= source[-1].t:
                value = source[-1].value
            else:
                while index + 1 < len(source) and source[index + 1].t < t:
                    index += 1
                a = source[index]
                b = source[index + 1]
                if b.t <= a.t:
                    value = a.value
                else:
                    ratio = (t - a.t) / (b.t - a.t)
                    value = a.value + (b.value - a.value) * ratio
            out.append(Sample(t, value))
        self.samples = out


@dataclass
class DataLog:
    name: str
    channels: dict[str, Channel] = field(default_factory=dict)
    metadata: dict[str, str] = field(default_factory=dict)

    @property
    def start(self) -> float:
        starts = [channel.start for channel in self.channels.values() if channel.samples]
        return min(starts) if starts else 0.0

    @property
    def end(self) -> float:
        ends = [channel.end for channel in self.channels.values() if channel.samples]
        return max(ends) if ends else 0.0

    def extract(self, start: float, end: float, *, rebase: bool = True) -> "DataLog":
        extracted = DataLog(self.name, metadata=dict(self.metadata))
        for key, channel in self.channels.items():
            child = channel.extract(start, end, rebase=rebase)
            if child.samples:
                extracted.channels[key] = child
        return extracted

    def resample(self, frequency_hz: float | None) -> None:
        start = self.start
        end = self.end
        for channel in self.channels.values():
            channel.resample(start, end, frequency_hz)
