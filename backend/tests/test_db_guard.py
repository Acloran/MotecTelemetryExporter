import pytest

from motec_exporter.db import ReadOnlyDatabase


def test_read_only_guard_allows_select_and_cte():
    ReadOnlyDatabase._assert_read_only("select * from packet")
    ReadOnlyDatabase._assert_read_only("with bounds as (select 1) select * from bounds")


@pytest.mark.parametrize(
    "sql",
    [
        "delete from packet",
        "update packet set time = 1",
        "insert into packet values (1, 1)",
        "drop table packet",
        "copy packet from stdin",
    ],
)
def test_read_only_guard_rejects_mutations(sql):
    with pytest.raises(ValueError):
        ReadOnlyDatabase._assert_read_only(sql)

