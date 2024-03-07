#!/usr/bin/env -S pytest -svv
import pytest

from io import StringIO
from digital_land.pipeline import run_pipeline
from digital_land.phase.load import LoadPhase
from digital_land.phase.parse import ParsePhase
from digital_land.phase.map import MapPhase
from digital_land.phase.map import normalise
from digital_land.phase.save import SavePhase


def TestPipeline(phase, inputs):
    output = StringIO()
    run_pipeline(
        LoadPhase(f=StringIO(inputs)), ParsePhase(), phase, SavePhase(f=output)
    )
    return output.getvalue()


def test_headers_empty_columns():
    m = MapPhase(["one", "two"])
    output = TestPipeline(m, "one,two\r\n1,2\r\n")
    assert output == ("one,two\r\n" "1,2\r\n")


def test_map_headers():
    m = MapPhase(["one", "two"], columns={"three": "two"})
    output = TestPipeline(m, "one,THREE\r\n1,3\r\n")
    assert output == "one,two\r\n1,3\r\n"


def test_map_straight():
    output = TestPipeline(MapPhase(["one", "two"]), "one,two\r\n1,2\r\n")
    assert output == "one,two\r\n1,2\r\n"


def test_map_headers_column_clash():
    m = MapPhase(["One"], {"une": "One", "ein": "One"})
    output = TestPipeline(m, "une,ein\r\n1,2\r\n")
    assert output == "One\r\n1\r\n"


def test_map_empty_geometry_column():
    m = MapPhase(
        [
            "categories",
            "conservation-area",
            "documentation-url",
            "end-date",
            "entity",
            "entry-date",
            "geometry",
            "legislation",
            "name",
            "notes",
            "organisation",
            "point",
            "prefix",
            "reference",
            "start-date",
        ],
        {
            "wkt": "geometry",
            "documenturl": "documentation-url",
            "url": "documentation-url",
        },
    )
    output = TestPipeline(
        m,
        "categories,conservation-area,documentation-url,end-date,entity,"
        "entry-date,WKT,legislation,name,notes,organisation,point,prefix,"
        "reference,start-date,geometry\r\n,,,,,,MULTIPOLYGON(),,,,,,,,,,\r\n",
    )
    assert (
        output == "categories,conservation-area,documentation-url,end-date,entity,"
        "entry-date,geometry,legislation,name,notes,organisation,point,prefix,"
        "reference,start-date\r\n,,,,,,MULTIPOLYGON(),,,,,,,,\r\n"
    )


@pytest.mark.parametrize(
    "column_name, expected",
    [
        ("hello_world", "hello-world"),
        ("hello-world", "hello-world"),
        ("Hello_World", "hello-world"),
        ("Hello-World", "hello-world"),
    ],
)
def test_map_normalize_removes_underscores(column_name, expected):
    actual = normalise(column_name)

    assert actual == expected


def test_map_column_names_with_underscores_when_column_not_in_specification():
    """
    This tests for successful mapping of resource columns not
    present in the specification schema fields
    :return:
    """
    fieldnames = ["Organisation_Label", "PermissionDate", "SiteNameAddress"]
    columns = {"address": "SiteNameAddress", "ownership": "OwnershipStatus"}

    m = MapPhase(fieldnames, columns)
    output = TestPipeline(
        m, "Organisation_Label,PermissionDate,test\r\ncol-1-val,col-2-val,\r\n"
    )
    assert output == "Organisation_Label,PermissionDate\r\ncol-1-val,col-2-val\r\n"


def test_map_column_names_with_underscores_when_column_in_specification():
    """
    This tests for successful mapping of resource columns that are
    present in the specification schema fields
    :return:
    """
    fieldnames = ["Organisation_Label", "end_date", "SiteNameAddress"]
    columns = {
        "organisation-label": "Organisation-Label",
        "end-date": "end-date",
        "ownership": "OwnershipStatus",
    }

    m = MapPhase(fieldnames, columns)
    output = TestPipeline(
        m, "Organisation_Label,end_date,SiteNameAddress\r\ncol-1-val,col-2-val,\r\n"
    )
    assert (
        output
        == "Organisation-Label,SiteNameAddress,end-date\r\ncol-1-val,,col-2-val\r\n"
    )


def test_null_to_empty_string():
    fieldnames = ["Nada"]
    columns = {}

    output = list(MapPhase(fieldnames, columns).process([{"row":{"Nada":None}}]))

    assert len(output) == 1
    assert output[0]['row']['Nada'] == ""
