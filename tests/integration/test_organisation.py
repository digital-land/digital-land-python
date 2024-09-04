from digital_land.organisation import Organisation
import csv
import logging


class TestOrganisation:
    def test_organisation_lookup(self):
        organisation = Organisation(
            organisation_path="tests/data/listed-building/organisation.csv",
            pipeline_dir=None,
        )

        assert organisation.lookup("Borchester") == ""
        assert (
            organisation.lookup("local-authority-eng:LBH") == "local-authority-eng:LBH"
        )
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
        assert organisation.lookup("Lambeth") == "local-authority-eng:LBH"

    def test_organisation_lookup_eng_removed(self, tmp_path):
        org_match = {
            "organisation": "development-corporation:Q20648596",
            "entity": 1,
            "wikidata": "Q20648596",
            "name": "Example",
            "statistical-geography": "E000001",
            "opendatacommunities": "http://opendatacommunities.org/id/dev-corp/old-oak-and-park-royal",
        }
        logging.warning(type(org_match))
        with open(tmp_path / "test.csv", "w") as f:
            dictwriter = csv.DictWriter(f, fieldnames=org_match.keys())
            dictwriter.writeheader()
            logging.warning(type(org_match))
            dictwriter.writerow(org_match)
        organisation = Organisation(
            organisation_path="tests/data/listed-building/organisation.csv",
            pipeline_dir=None,
        )
        assert (
            organisation.lookup("local-authority-eng:BRW") == "local-authority-eng:BRW"
        )
