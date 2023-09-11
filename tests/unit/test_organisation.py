from digital_land.organisation import Organisation
from digital_land.phase.organisation import OrganisationPhase
from digital_land.log import IssueLog


def test_organisation_lookup():
    organisation = Organisation(
        organisation_path="tests/data/listed-building/organisation.csv"
    )

    assert organisation.lookup("Borchester") == ""
    assert organisation.lookup("local-authority-eng:LBH") == "local-authority-eng:LBH"
    assert (
        organisation.lookup("government-organisation:PB1164")
        == "government-organisation:PB1164"
    )
    assert (
        organisation.lookup(
            "http://opendatacommunities.org/id/district-council/brentwood"
        )
        == "local-authority-eng:BRW"
    )

    assert (
        organisation.lookup(
            "http://opendatacommunities.org/id/district-council/tamworth"
        )
        == "local-authority-eng:TAW"
    )
    assert (
        organisation.lookup(
            "http://opendatacommunities.org/doc/district-council/tamworth"
        )
        == "local-authority-eng:TAW"
    )
    assert (
        organisation.lookup(
            "https://opendatacommunities.org/id/district-council/tamworth"
        )
        == "local-authority-eng:TAW"
    )
    assert (
        organisation.lookup(
            "https://opendatacommunities.org/doc/district-council/tamworth"
        )
        == "local-authority-eng:TAW"
    )

    assert organisation.lookup("E07000068") == "local-authority-eng:BRW"


def test_process_with_valid_organisation():
    organisation = Organisation(
        organisation_path="tests/data/listed-building/organisation.csv"
    )
    assert organisation.lookup("Lambeth") == "local-authority-eng:LBH"
    input_stream = [
        {
            "row": {
                "organisation": "Lambeth",
            }
        }
    ]
    organisationPhase = OrganisationPhase(organisation=organisation, issues=None)
    output = [block for block in organisationPhase.process(input_stream)]
    assert output[0]["row"]["organisation"] == "local-authority-eng:LBH"


def test_process_with_missing_organisation():
    organisation = Organisation(
        organisation_path="tests/data/listed-building/organisation.csv"
    )
    assert organisation.lookup("Borchester") == ""
    issues = IssueLog(dataset="brownfield-land", resource="1")
    input_stream = [
        {
            "row": {
                "organisation": "Borchester",
            },
            "resource": "1",
            "line-number": 1,
            "entry-number": 2,
        }
    ]
    organisationPhase = OrganisationPhase(organisation=organisation, issues=issues)
    output = [block for block in organisationPhase.process(input_stream)]
    assert output[0]["row"]["organisation"] == ""
