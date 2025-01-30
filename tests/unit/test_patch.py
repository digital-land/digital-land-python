#!/usr/bin/env -S py.test -svv

from digital_land.phase.patch import PatchPhase
from digital_land.log import IssueLog


def test_patch_regex():
    issues = IssueLog()

    patches = {
        "grade": {
            "^1$": "I",
            "^2$": "II",
            "^2\\*$": "II*",
            "^2 Star$": "II*",
            "^3$": "III",
        },
        "OrganisationURI": {
            "https://example.com/search?query=data&filter=name%20contains%20test": "patch_organisation",
        },
    }

    p = PatchPhase(patches=patches, issues=issues)

    assert p.apply_patch("grade", "II") == "II"
    assert issues.rows == []

    assert p.apply_patch("grade", "II*") == "II*"
    assert issues.rows == []

    assert p.apply_patch("grade", "2") == "II"

    issue = issues.rows.pop()
    assert issue["field"] == "grade"
    assert issue["issue-type"] == "patch"
    assert issue["value"] == "2"
    assert issues.rows == []

    assert p.apply_patch("grade", "2*") == "II*"

    issue = issues.rows.pop()
    assert issue["field"] == "grade"
    assert issue["issue-type"] == "patch"
    assert issue["value"] == "2*"
    assert issues.rows == []

    assert p.apply_patch("grade", "2 Star") == "II*"

    issue = issues.rows.pop()
    assert issue["field"] == "grade"
    assert issue["issue-type"] == "patch"
    assert issue["value"] == "2 Star"
    assert issues.rows == []

    assert (
        p.apply_patch(
            "OrganisationURI",
            "https://example.com/search?query=data&filter=name%20contains%20test",
        )
        == "patch_organisation"
    )
    issue = issues.rows.pop()
    assert issue["field"] == "OrganisationURI"
    assert issue["issue-type"] == "patch"
    assert (
        issue["value"]
        == "https://example.com/search?query=data&filter=name%20contains%20test"
    )
    assert issues.rows == []
