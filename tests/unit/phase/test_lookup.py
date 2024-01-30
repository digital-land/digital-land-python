import pytest

from digital_land.phase.lookup import LookupPhase


class TestLookupPhase:
    @pytest.mark.parametrize(
        "entry_organisation", ["local-authority-eng:DNC", "local-authority:DNC"]
    )
    def test_process_organisation_is_matched(self, entry_organisation):
        input_stream = [
            {
                "row": {
                    "prefix": "dataset",
                    "reference": "1",
                    "organisation": entry_organisation,
                },
                "entry-number": 1,
            }
        ]
        lookups = {",dataset,1,local-authoritydnc": "1"}
        phase = LookupPhase(lookups=lookups)

        phase.entity_field = "entity"

        output = [block for block in phase.process(input_stream)]

        assert output[0]["row"]["entity"] == "1"
