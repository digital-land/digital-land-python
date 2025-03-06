from digital_land.phase.migrate import MigratePhase

field_schema = ['EndDate', 'OrganisationURI', 'entity', 'entry-date', 'organisation', 'start-date']

migrations = {'organisation': 'OrganisationURI', 'end-date': 'EndDate'}


# Checking that fields in field_schema don't get added to the output and that None's are removed
def test_migrate_phase():
    # Mock input stream
    input_stream = iter([
        {"row": {"organisation": "local-organisation:ABC", "start-date": "2022-02-02",  "entity": 12345678}},
        {"row": {"organisation": "local-organisation:DEF", "start-date": "2022-03-02",  "entity": None}},
        {"row": {"organisation": "local-organisation:MNO", "start-date": "2022-06-02"}},
        {"row": {}}
    ])

    # Initialize the phase
    migrate_phase = MigratePhase(fields=field_schema, migrations=migrations)

    # Run process
    output = list(migrate_phase.process(input_stream))
    print(output)

    # Assertions
    assert output[0]["row"] == {
        'organisation': 'local-organisation:ABC',
        'entity': 12345678,
        'start-date': '2022-02-02'
    }, f"First row not outputted correctly"
    assert output[1]["row"] == {
        'organisation': 'local-organisation:DEF',
        'start-date': '2022-03-02'
    }, f"Second row not outputted correctly"
    assert output[2]["row"] == {
        'organisation': 'local-organisation:MNO',
        'start-date': '2022-06-02'
    }, f"Third row not outputted correctly"
    assert output[3]["row"] == {}, f"Final row should not have elements"

    for i in range(len(output)):
        assert "EndDate" not in output[i]["row"]
        assert "end-date" not in output[i]["row"]
