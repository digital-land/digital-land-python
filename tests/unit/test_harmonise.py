#!/usr/bin/env -S py.test -svv

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


def test_harmonise_apply_patch():
    specification = Specification("tests/data/specification")
    issues = IssueLog()

    patches = {"field-string": {"WRONG": "right", "same": "same"}}

    h = HarmonisePhase(specification=specification, patches=patches, issues=issues)

    assert h.apply_patch("field-string", "right") == "right"
    assert h.apply_patch("field-string", "WRONG") == "right"
    assert h.apply_patch("field-string", "same") == "same"

    issue = issues.rows.pop()
    assert issue["field"] == "field-string"
    assert issue["issue-type"] == "patch"
    assert issue["value"] == "WRONG"
    assert issues.rows == []


def test_harmonise():
    specification = Specification("tests/data/specification")
    issues = IssueLog()

    h = HarmonisePhase(specification=specification, issues=issues)
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
