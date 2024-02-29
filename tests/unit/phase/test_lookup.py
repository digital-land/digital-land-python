import pytest

from digital_land.phase.lookup import LookupPhase, PrintLookupPhase


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


def test_process_410_redirect():
    input_stream = [
        {
            "row": {
                "prefix": "dataset",
                "reference": "1",
                "organisation": "test",
            },
            "entry-number": 1,
        }
    ]
    lookups = {",dataset,1,test": "1"}
    redirect_lookups = {"1": {"entity": "", "status": "410"}}
    phase = PrintLookupPhase(lookups=lookups, redirect_lookups=redirect_lookups)
    output = [block for block in phase.process(input_stream)]

    assert output[0]["row"]["entity"] == ""


def test_process_301_redirect():
    input_stream = [
        {
            "row": {
                "prefix": "dataset",
                "reference": "1",
                "organisation": "test",
            },
            "entry-number": 1,
        }
    ]
    lookups = {",dataset,1,test": "1"}
    redirect_lookups = {"1": {"entity": "2", "status": "301"}}
    phase = PrintLookupPhase(lookups=lookups, redirect_lookups=redirect_lookups)
    output = [block for block in phase.process(input_stream)]
    assert output[0]["row"]["entity"] == "2"


def test_process_successful_lookup():
    input_stream = [
        {
            "row": {
                "prefix": "dataset",
                "reference": "1",
                "organisation": "test",
            },
            "entry-number": 1,
        }
    ]
    lookups = {",dataset,1,test": "1"}
    phase = PrintLookupPhase(lookups=lookups)
    output = [block for block in phase.process(input_stream)]
    assert output[0]["row"]["entity"] == "1"
