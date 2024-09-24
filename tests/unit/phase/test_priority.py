import pytest

from digital_land.phase.priority import PriorityPhase


def test_process_no_config():
    input_stream = [
        {
            "row": {
                "prefix": "dataset",
                "reference": "1",
                "organisation": "local-authority-eng:DNC",
            },
            "entry-number": 1,
            "line-number": 2,
        }
    ]
    phase = PriorityPhase()
    output = [block for block in phase.process(input_stream)]

    assert output[0]["priority"] == phase.default_priority


@pytest.mark.parametrize(
    "organisation, expected_priority",
    [("local-authority:DNC", 1), ("local-authority:NOO", 2)],
)
def test_process_config_provided(mocker, organisation, expected_priority):
    config = mocker.Mock()
    config.get_entity_organisation.return_value = "local-authority:DNC"
    input_stream = [
        {
            "row": {
                "prefix": "dataset",
                "reference": "1",
                "organisation": organisation,
                "entity": 1,
            },
            "entry-number": 1,
            "line-number": 2,
        }
    ]
    phase = PriorityPhase(config=config)
    output = [block for block in phase.process(input_stream)]

    assert output[0]["priority"] == expected_priority
