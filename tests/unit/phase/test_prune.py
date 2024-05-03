from digital_land.phase.prune import EntityPrunePhase
from digital_land.log import IssueLog, DatasetResourceLog


class TestEntityPrunePhase:
    def test_process_returns_missing_reference(self):
        input_stream = [
            {
                "row": {
                    "prefix": "ancient-woodland",
                    "reference": "",
                    "entity": "",
                },
                "entry-number": 1,
                "resource": "123",
                "line-number": 2,
            }
        ]
        issues = IssueLog()
        resource_log = DatasetResourceLog()
        phase = EntityPrunePhase(issues, resource_log)

        list(phase.process(input_stream))
        assert issues.rows[0]["issue-type"] == "unknown entity - missing reference"

    def test_process_returns_unknown_entity(self):
        input_stream = [
            {
                "row": {
                    "prefix": "dataset",
                    "reference": "REF01",
                    "entity": "",
                },
                "entry-number": 1,
                "resource": "123",
                "line-number": 2,
            }
        ]
        issues = IssueLog()
        resource_log = DatasetResourceLog()
        phase = EntityPrunePhase(issues, resource_log)

        list(phase.process(input_stream))
        assert issues.rows[0]["issue-type"] == "unknown entity"
