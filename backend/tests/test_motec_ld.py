import struct
import xml.etree.ElementTree as ET
from array import array

from motec_exporter.datalog import Channel, DataLog, Sample
from motec_exporter.motec_ld import CHAN_FMT, CHANNEL_HEADER_SIZE, HEADER_PTR, HEAD_FMT, write_ld, write_ldx


def test_write_ld_packs_channel_headers_and_float_data(tmp_path):
    path = tmp_path / "sample.ld"
    log = DataLog(
        "sample",
        channels={
            "motor_rpm": Channel("Motor RPM", "rpm", "speed", [Sample(0.0, 1000.0), Sample(0.02, 2000.0)]),
            "pack_v": Channel("HV Pack Voltage", "V", "voltage", [Sample(0.0, 310.0), Sample(0.02, 311.0)]),
        },
    )

    write_ld(path, log, metadata={"vehicle_id": "Orion", "event": "Unit Test"})

    blob = path.read_bytes()
    header = struct.unpack(HEAD_FMT, blob[: struct.calcsize(HEAD_FMT)])
    assert header[1] == HEADER_PTR
    assert header[2] == HEADER_PTR + 2 * CHANNEL_HEADER_SIZE
    assert header[11] == 2

    first = struct.unpack(CHAN_FMT, blob[HEADER_PTR : HEADER_PTR + CHANNEL_HEADER_SIZE])
    second = struct.unpack(CHAN_FMT, blob[HEADER_PTR + CHANNEL_HEADER_SIZE : HEADER_PTR + 2 * CHANNEL_HEADER_SIZE])
    assert first[0] == 0
    assert first[1] == HEADER_PTR + CHANNEL_HEADER_SIZE
    assert second[0] == HEADER_PTR
    assert second[1] == 0
    assert first[3] == 2
    assert first[12].split(b"\0", 1)[0] == b"Motor RPM"
    assert first[13].split(b"\0", 1)[0] == b"speed"
    assert first[14].split(b"\0", 1)[0] == b"rpm"

    first_values = array("f")
    first_values.frombytes(blob[first[2] : first[2] + first[3] * 4])
    assert list(first_values) == [1000.0, 2000.0]


def test_write_ldx_uses_motec_marker_group_schema(tmp_path):
    path = tmp_path / "sample.ldx"

    write_ldx(path, [5.0, 5.0001, 15.0], [2.0])

    root = ET.parse(path).getroot()
    assert root.tag == "LDXFile"
    marker_group = root.find("./Layers/Layer/MarkerBlock/MarkerGroup")
    assert marker_group is not None
    assert marker_group.attrib["Name"] == "Beacons"
    markers = marker_group.findall("Marker")
    assert [marker.attrib["ClassName"] for marker in markers] == ["SPLTBCN", "BCN", "BCN"]
    assert [marker.attrib["Time"] for marker in markers] == ["2000000.000000", "5000000.000000", "15000000.000000"]

