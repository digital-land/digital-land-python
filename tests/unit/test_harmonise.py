#!/usr/bin/env -S py.test -svv

from digital_land.organisation import Organisation
from digital_land.phase.harmonise import HarmonisePhase
from digital_land.specification import Specification
from digital_land.log import IssueLog

from .conftest import FakeDictReader


def test_harmonise_field():
    specification = Specification("tests/data/specification")
    issues = IssueLog()

    h = HarmonisePhase(specification=specification, issues=issues)

    assert h.harmonise_field("field-string", None) == ""
    assert h.harmonise_field("field-string", "value") == "value"


def test_harmonise():
    specification = Specification("tests/data/specification")
    organisation = Organisation("tests/data/organisation.csv")
    issues = IssueLog()

    h = HarmonisePhase(
        specification=specification, issues=issues, organisation=organisation
    )
    reader = FakeDictReader(
        [
            {"field-integer": "123"},
            {"field-integer": "  321   "},
            {"field-integer": "hello"},
        ]
    )
    output = list(h.process(reader))
    assert len(output) == 3
    assert output[0]["row"] == {"field-integer": "123"}, "pass through valid data"
    assert output[1]["row"] == {"field-integer": "321"}, "whitespace trimmed"
    assert output[2]["row"] == {"field-integer": ""}, "remove bad data"


def test_harmonise_inside_custom_boundary():
    specification = Specification("tests/data/specification")
    organisation = Organisation(
        "tests/data/boundary/organisation.csv",
        None,
        None,
        "tests/data/boundary/la_geometry.csv",
    )
    issues = IssueLog()

    h = HarmonisePhase(
        specification=specification, issues=issues, organisation=organisation
    )
    reader = FakeDictReader(
        [
            {
                "organisation": "local-authority-eng:HPL",
                "GeoX": "-1.259419",
                "GeoY": "54.66946",
            },
        ],
        None,
        dataset="brownfield-land",
    )
    output = list(h.process(reader))
    assert len(output) == 1
    assert output[0]["row"]["GeoX"] == "-1.259419"
    assert output[0]["row"]["GeoY"] == "54.66946"


def test_harmonise_outside_custom_boundary():
    specification = Specification("tests/data/specification")
    organisation = Organisation(
        "tests/data/boundary/organisation.csv",
        None,
        None,
        "tests/data/boundary/la_geometry.csv",
    )
    issues = IssueLog()

    h = HarmonisePhase(
        specification=specification, issues=issues, organisation=organisation
    )
    reader = FakeDictReader(
        [
            {
                "organisation": "local-authority-eng:HPL",
                "GeoX": "-0.446154",
                "GeoY": "53.081202",
            },
        ],
        None,
        "brownfield-land",
    )
    output = list(h.process(reader))
    assert len(output) == 1
    assert output[0]["row"]["GeoX"] == ""
    assert output[0]["row"]["GeoY"] == ""


def test_harmonise_non_brownfield_outside_custom_boundary():
    specification = Specification("tests/data/specification")
    organisation = Organisation(
        "tests/data/boundary/organisation.csv",
        None,
        None,
        "tests/data/boundary/la_geometry.csv",
    )
    issues = IssueLog()

    h = HarmonisePhase(
        specification=specification, issues=issues, organisation=organisation
    )
    reader = FakeDictReader(
        [
            {
                "organisation": "local-authority-eng:HPL",
                "GeoX": "-0.446154",
                "GeoY": "53.081202",
            },
        ],
        None,
        "not-brownfield-land",
    )
    output = list(h.process(reader))
    assert len(output) == 1
    assert output[0]["row"]["GeoX"] == "-0.446154"
    assert output[0]["row"]["GeoY"] == "53.081202"


def test_harmonise_inside_default_boundary_unknown_organisation():
    specification = Specification("tests/data/specification")
    organisation = Organisation(
        "tests/data/boundary/organisation.csv",
        None,
        None,
        "tests/data/boundary/la_geometry.csv",
    )
    issues = IssueLog()

    h = HarmonisePhase(
        specification=specification, issues=issues, organisation=organisation
    )
    reader = FakeDictReader(
        [
            {
                "organisation": "???",
                "GeoX": "-0.446154",
                "GeoY": "53.081202",
            },
        ],
        None,
        "brownfield-land",
    )
    output = list(h.process(reader))
    assert len(output) == 1
    assert output[0]["row"]["GeoX"] == "-0.446154"
    assert output[0]["row"]["GeoY"] == "53.081202"


def test_harmonise_outside_default_boundary_unknown_organisation():
    specification = Specification("tests/data/specification")
    organisation = Organisation(
        "tests/data/boundary/organisation.csv",
        None,
        None,
        "tests/data/boundary/la_geometry.csv",
    )
    issues = IssueLog()

    h = HarmonisePhase(
        specification=specification, issues=issues, organisation=organisation
    )
    reader = FakeDictReader(
        [
            {
                "organisation": "???",
                "GeoX": "7.578883",
                "GeoY": "54.379565",
            },
        ],
        None,
        "brownfield-land",
    )
    output = list(h.process(reader))
    assert len(output) == 1
    assert output[0]["row"]["GeoX"] == ""
    assert output[0]["row"]["GeoY"] == ""
