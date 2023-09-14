from digital_land.phase.organisation import OrganisationPhase
from digital_land.log import IssueLog
from unittest.mock import Mock


def test_process_with_valid_organisation(mocker):
    mock_organisation = Mock()

    # Mock the `lookup` method to return a predefined value
    mock_organisation.lookup.return_value = "local-authority-eng:LBH"

    mocker.patch("digital_land.phase.organisation", return_value=mock_organisation)

    input_stream = [
        {
            "row": {
                "organisation": "http://opendatacommunities.org/id/london-borough-council/lambeth",
            }
        }
    ]

    organisationPhase = OrganisationPhase(organisation=mock_organisation, issues=None)
    output = [block for block in organisationPhase.process(input_stream)]
    assert not organisationPhase.issues
    assert output[0]["row"]["organisation"] == "local-authority-eng:LBH"


def test_process_with_invalid_organisation(mocker):
    mock_organisation = Mock()

    # Mock the `lookup` method to return a predefined value
    mock_organisation.lookup.return_value = ""

    mocker.patch("digital_land.phase.organisation", return_value=mock_organisation)

    issues = IssueLog(dataset="brownfield-land", resource="1")
    input_stream = [
        {
            "row": {
                "organisation": "http://open datacommunities.org/not_a_valid_organisation",
            },
            "resource": "1",
            "line-number": 1,
            "entry-number": 2,
        }
    ]
    organisationPhase = OrganisationPhase(organisation=mock_organisation, issues=issues)
    output = [block for block in organisationPhase.process(input_stream)]
    assert organisationPhase.issues
    assert output[0]["row"]["organisation"] == ""
