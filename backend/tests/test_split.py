from motec_exporter.datalog import Channel, Sample
from motec_exporter.config import Settings
from motec_exporter.sample_data import BASE_MS, SESSION_LENGTH_MS
from motec_exporter.split import detect_split_ranges
from motec_exporter.telemetry import TelemetryService


def test_detect_split_ranges_with_rpm_stops():
    channel = Channel(
        "Motor RPM",
        "rpm",
        samples=[
            Sample(0, 0),
            Sample(10, 2600),
            Sample(20, 2800),
            Sample(30, 2500),
            Sample(40, 700),
            Sample(55, 650),
            Sample(60, 3000),
            Sample(70, 3200),
            Sample(80, 650),
            Sample(95, 600),
            Sample(100, 3400),
            Sample(110, 3600),
            Sample(120, 650),
        ],
    )

    assert detect_split_ranges(channel) == [(10, 30), (60, 70), (100, 110)]


def test_threshold_segments_honor_minimum_duration():
    service = TelemetryService(Settings(TELEMETRY_SOURCE="sample"))

    segments = service.threshold_segments(BASE_MS, BASE_MS + SESSION_LENGTH_MS, "motor_rpm", threshold=1000, min_duration_s=250)

    assert len(segments) == 1
    assert segments[0].duration_s >= 250
