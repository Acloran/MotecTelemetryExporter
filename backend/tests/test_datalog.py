from motec_exporter.datalog import Channel, DataLog, Sample


def test_resample_clamps_sparse_channels_without_extrapolating():
    log = DataLog(
        "sample",
        channels={
            "early": Channel("Early", samples=[Sample(0.0, 0.0), Sample(1.0, 10.0)]),
            "late": Channel("Late", samples=[Sample(0.5, 100.0), Sample(1.0, 200.0)]),
        },
    )

    log.resample(2)

    assert [sample.value for sample in log.channels["early"].samples] == [0.0, 5.0, 10.0]
    assert [sample.value for sample in log.channels["late"].samples] == [100.0, 100.0, 200.0]

