from motec_exporter.channel_chart_store import ChannelChartStore, apply_channel_chart
from motec_exporter.datalog import Channel, DataLog, Sample
from motec_exporter.models import ChannelChartDefinition, ChannelChartEntry


def test_channel_chart_store_round_trip_and_applies_case_insensitive_names(tmp_path):
    store = ChannelChartStore(tmp_path)
    saved = store.save(
        ChannelChartDefinition(
            name="Orion Default",
            slug="Orion Default",
            entries=[ChannelChartEntry(channel_name="Motor Speed", quantity_type="Angular Speed", unit="rpm")],
        )
    )
    assert saved.slug == "orion-default"

    log = DataLog(
        "test",
        channels={"motor": Channel(" motor   speed ", "", "", [Sample(0.0, 1.0)])},
    )
    matched = apply_channel_chart(log, store.load("orion-default"))

    assert matched == 1
    assert log.channels["motor"].quantity == "Angular Speed"
    assert log.channels["motor"].unit == "rpm"
