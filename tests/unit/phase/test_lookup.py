import pytest

from digital_land.phase.lookup import LookupPhase, EntityLookupPhase, PrintLookupPhase
from digital_land.log import IssueLog


@pytest.fixture
def get_input_stream():
    return [
        {
            "row": {
                "prefix": "dataset",
                "reference": "1",
                "organisation": "test",
            },
            "entry-number": 1,
            "line-number": 2,
        }
    ]


@pytest.fixture
def get_input_stream_with_linked_field():
    return [
        {
            "row": {
                "prefix": "article-4-direction-area",
                "reference": "1",
                "organisation": "local-authority:ABC",
                "article-4-direction": "a4d2",
            },
            "entry-number": 1,
            "line-number": 2,
        }
    ]


@pytest.fixture
def get_lookup():
    return {",dataset,1,test": "1"}


class TestLookupPhase:
    def test_redirect_entity_entity_is_not_redirected(self):
        redirects = {"101": {"entity": "101", "status": "310"}}
        phase = LookupPhase(redirect_lookups=redirects)
        input_entity = "401"
        output_entity = phase.redirect_entity(input_entity)
        assert output_entity == input_entity

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
                "line-number": 2,
            }
        ]
        lookups = {",dataset,1,local-authoritydnc": "1"}
        phase = LookupPhase(lookups=lookups)
        phase.entity_field = "entity"
        output = [block for block in phase.process(input_stream)]

        assert output[0]["row"]["entity"] == "1"

    def test_process_entity_removed(self, get_input_stream, get_lookup):
        input_stream = get_input_stream
        lookups = get_lookup
        issues = IssueLog()
        redirect_lookups = {"1": {"entity": "", "status": "410"}}
        phase = LookupPhase(
            lookups=lookups, redirect_lookups=redirect_lookups, issue_log=issues
        )
        phase.entity_field = "entity"
        output = [block for block in phase.process(input_stream)]

        # no issue raised for removed entity
        assert output[0]["row"]["entity"] == ""
        assert len(issues.rows) == 0

    def test_process_raise_issue(self, get_input_stream):
        input_stream = get_input_stream
        lookups = {",ancient-woodland,1,test": "1"}
        issues = IssueLog()
        redirect_lookups = {"10": {"entity": "", "status": "410"}}
        phase = LookupPhase(
            lookups=lookups, redirect_lookups=redirect_lookups, issue_log=issues
        )
        phase.entity_field = "entity"
        output = [block for block in phase.process(input_stream)]

        assert output[0]["row"]["entity"] == ""
        assert issues.rows[0]["issue-type"] == "unknown entity"

    def test_process_raise_range_issue(self, get_input_stream):
        input_stream = get_input_stream
        lookups = {",dataset,1,test": "1"}
        issues = IssueLog()
        phase = LookupPhase(
            lookups=lookups,
            redirect_lookups={},
            issue_log=issues,
            entity_range=["10", "20"],
        )
        phase.entity_field = "entity"
        output = [block for block in phase.process(input_stream)]

        assert output[0]["row"]["entity"] == "1"
        assert issues.rows[0]["issue-type"] == "entity number out of range"

    def test_process_empty_prefix(self, get_lookup):
        input_stream = [
            {
                "row": {
                    "prefix": "",
                    "reference": "1",
                    "organisation": "test",
                    "entity": "10",
                },
                "entry-number": 1,
                "line-number": 2,
            }
        ]
        lookups = get_lookup
        issues = IssueLog()
        redirect_lookups = {}
        phase = LookupPhase(
            lookups=lookups, redirect_lookups=redirect_lookups, issue_log=issues
        )
        phase.entity_field = "entity"
        output = [block for block in phase.process(input_stream)]

        assert output[0]["row"]["entity"] == "10"

    def test_no_associated_documents_issue(self, get_input_stream_with_linked_field):
        input_stream = get_input_stream_with_linked_field

        lookups = {
            ",article-4-direction,a4d1,local-authorityabc": "1",
            ",article-4-direction-area,1,local-authorityabc": "2",
        }
        issues = IssueLog()

        phase = LookupPhase(
            lookups=lookups,
            issue_log=issues,
        )
        phase.entity_field = "entity"
        output = [block for block in phase.process(input_stream)]

        assert output[0]["row"]["entity"] == "2"
        assert (
            issues.rows[0]["issue-type"]
            == "no associated documents found for this area"
        )
        assert issues.rows[0]["value"] == "a4d2"

    def test_no_associated_documents_issue_for_retired_entity(
        self, get_input_stream_with_linked_field
    ):
        input_stream = get_input_stream_with_linked_field

        lookups = {
            ",article-4-direction,a4d2,local-authorityabc": "1",
            ",article-4-direction-area,1,local-authorityabc": "2",
        }
        issues = IssueLog()
        redirect_lookups = {"1": {"entity": "", "status": "410"}}

        phase = LookupPhase(
            lookups=lookups,
            redirect_lookups=redirect_lookups,
            issue_log=issues,
        )
        phase.entity_field = "entity"
        output = [block for block in phase.process(input_stream)]

        assert output[0]["row"]["entity"] == "2"
        assert (
            issues.rows[0]["issue-type"]
            == "no associated documents found for this area"
        )
        assert issues.rows[0]["value"] == "a4d2"


class TestPrintLookupPhase:
    def test_process_does_not_produce_new_lookup(self, get_input_stream, get_lookup):
        input_stream = get_input_stream
        lookups = get_lookup
        redirect_lookups = {"1": {"entity": "", "status": "410"}}
        phase = PrintLookupPhase(lookups=lookups, redirect_lookups=redirect_lookups)
        [block for block in phase.process(input_stream)]

        assert len(phase.new_lookup_entries) == 0


class TestEntityLookupPhase:
    def test_entity_lookup_phase(self):
        input_stream = [
            {
                "row": {
                    "prefix": "dataset",
                    "reference": "1",
                    "organisation": "local-authority:DNC",
                },
                "entry-number": 1,
                "line-number": 2,
            }
        ]
        lookups = {",dataset,1,local-authoritydnc": "1"}
        phase = EntityLookupPhase(lookups=lookups)
        phase.entity_field = "entity"
        output = [block for block in phase.process(input_stream)]

        assert output[0]["row"]["entity"] == "1"

    def test_entity_lookup_phase_two(self):
        input_stream = [
            {
                "row": {
                    "prefix": "dataset",
                    "reference": "1",
                    "organisation": "local-authority:DNC",
                },
                "entry-number": 1,
                "line-number": 2,
            },
            {
                "row": {
                    "prefix": "dataset",
                    "reference": "2",
                    "organisation": "local-authority:DNC",
                },
                "entry-number": 2,
                "line-number": 2,
            },
        ]
        lookups = {
            ",dataset,1,local-authoritydnc": "1",
            ",dataset,2,local-authoritydnc": "2",
        }
        phase = EntityLookupPhase(lookups=lookups)
        phase.entity_field = "entity"
        output = [block for block in phase.process(input_stream)]

        assert output[0]["row"]["entity"] == "1"
        assert output[1]["row"]["entity"] == "2"

    def test_entity_lookup_phase_blank(self):
        phase = EntityLookupPhase()
        input_stream = []
        lookups = {",dataset,1,local-authoritydnc": "1"}
        phase = EntityLookupPhase(lookups=lookups)
        phase.entity_field = "entity"
        output = [block for block in phase.process(input_stream)]

        assert len(output) == 0
