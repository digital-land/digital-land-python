import pytest
from digital_land.log import IssueLog
from digital_land.datatype.organisation import OrganisationURIDataType


@pytest.mark.skip()
def test_OrganisationURI_normalise():
    issues = IssueLog()

    organisation_uri = OrganisationURIDataType()

    assert "grape" not in organisation_uri.enum

    assert (
        organisation_uri.normalise(
            "http://opendatacommunities.org/id/district-council/brentwood"
        )
        == "http://opendatacommunities.org/id/district-council/brentwood"
    )
    assert (
        organisation_uri.normalise("brentwood")
        == "http://opendatacommunities.org/id/district-council/brentwood"
    )
    assert (
        organisation_uri.normalise("E07000068")
        == "http://opendatacommunities.org/id/district-council/brentwood"
    )

    assert organisation_uri.normalise("foo") == ""

    assert organisation_uri.normalise("foo", issues=issues) == ""
    issue = issues.rows.pop()
    assert issue["issue-type"] == "OrganisationURI"
    assert issue["value"] == "foo"
    assert issues.rows == []

    with pytest.raises(ValueError):
        organisation_uri.add_value(
            "http://opendatacommunities.org/id/district-council/borchester", "ambridge"
        )
