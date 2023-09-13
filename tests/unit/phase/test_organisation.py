from digital_land.organisation import Organisation
from digital_land.phase.organisation import OrganisationPhase
from digital_land.log import IssueLog


def test_process_with_valid_organisation():
    organisation = Organisation(
        organisation_path="tests/data/listed-building/organisation.csv"
    )
    input_stream = [
        {
            "row": {
                "organisation": "http://opendatacommunities.org/id/london-borough-council/lambeth",
            }
        }
    ]

    organisationPhase = OrganisationPhase(organisation=organisation, issues=None)
    output = [block for block in organisationPhase.process(input_stream)]
    assert output[0]["row"]["organisation"] == "local-authority-eng:LBH"


def test_process_with_invalid_organisation():
    organisation = Organisation(
        organisation_path="tests/data/listed-building/organisation.csv"
    )
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
    organisationPhase = OrganisationPhase(organisation=organisation, issues=issues)
    output = [block for block in organisationPhase.process(input_stream)]
    assert output[0]["row"]["organisation"] == ""
