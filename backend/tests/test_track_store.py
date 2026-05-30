from motec_exporter.models import GateLine, TrackDefinition
from motec_exporter.track_store import TrackStore


def test_track_store_round_trip(tmp_path):
    store = TrackStore(tmp_path)
    saved = store.save(
        TrackDefinition(
            name="COTA Lot J",
            slug="COTA Lot J",
            gates=[
                GateLine(
                    id="sf",
                    label="Start",
                    role="start_finish",
                    lat1=30.0,
                    lon1=-97.0,
                    lat2=30.1,
                    lon2=-97.1,
                )
            ],
        )
    )

    assert saved.slug == "cota-lot-j"
    loaded = store.load("cota-lot-j")
    assert loaded.gates[0].role == "start_finish"

