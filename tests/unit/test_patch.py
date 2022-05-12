#!/usr/bin/env -S py.test -svv

from digital_land.phase.patch import PatchPhase
from digital_land.log import IssueLog


def test_patch_phase():
    issues = IssueLog()

    patches = {"field-string": {"WRONG": "right", "same": "same"}}

    p = PatchPhase(patches=patches, issues=issues)

    assert p.apply_patch("field-string", "right") == "right"
    assert p.apply_patch("field-string", "WRONG") == "right"
    assert p.apply_patch("field-string", "same") == "same"

    issue = issues.rows.pop()
    assert issue["field"] == "field-string"
    assert issue["issue-type"] == "patch"
    assert issue["value"] == "WRONG"
    assert issues.rows == []
