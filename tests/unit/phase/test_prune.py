from digital_land.phase.prune import FieldPrunePhase

field_schema = [
    "EndDate",
    "OrganisationURI",
    "entity",
    "entry-date",
    "organisation",
    "start-date",
]


# Checking that None's are removed and limited to fields in field_schema
def test_prune_phase():
    # Mock input stream
    input_stream = iter(
        [
            {
                "row": {
                    "organisation": "local-org:ABC",
                    "start-date": "2022-02-02",
                    "entity": 12345678,
                    "test": "XYZ",
                }
            },
            {
                "row": {
                    "organisation": None,
                    "start-date": "2022-03-02",
                    "entity": 34567812,
                    "mock-column": "AAA",
                }
            },
            {
                "row": {
                    "organisation": "local-org:MNO",
                    "start-date": "2022-06-02",
                    "entity": 56781234,
                    "mock-column": None,
                }
            },
        ]
    )

    # Initialize the phase
    prune_phase = FieldPrunePhase(fields=field_schema)

    # Run process
    output = list(prune_phase.process(input_stream))

    # Assertions
    assert output[0]["row"] == {
        "entity": 12345678,
        "organisation": "local-org:ABC",
        "start-date": "2022-02-02",
    }, f"First row not outputted correctly"
    assert output[1]["row"] == {
        "entity": 34567812,
        "start-date": "2022-03-02",
    }, f"Second row not outputted correctly"
    assert output[2]["row"] == {
        "entity": 56781234,
        "organisation": "local-org:MNO",
        "start-date": "2022-06-02",
    }, f"Third row not outputted correctly"
