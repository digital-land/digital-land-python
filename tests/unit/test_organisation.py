from digital_land.organisation import Organisation


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
