# tests/unit/phase/test_harmonise.py
import pathlib
import sys

# Make repo root importable: tests/unit/phase/test_harmonise.py -> repo root
repo_root = pathlib.Path(__file__).resolve().parents[2]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from digital_land.phase.harmonise import HarmonisePhase  # noqa: E402


class FakeIssues:
    def __init__(self):
        self.logged = []
        self.resource = None
        self.line_number = None
        self.entry_number = None
        self.fieldname = None

    def log_issue(self, field, issue_type, value, message=None):
        self.logged.append(
            {
                "field": field,
                "issue_type": issue_type,
                "value": value,
                "message": message,
            }
        )


def make_block(row):
    return {
        "resource": "test.csv",
        "line-number": 1,
        "entry-number": 1,
        "row": row.copy(),
    }


def run_phase(dataset, row, valid_category_values=None):
    issues = FakeIssues()
    phase = HarmonisePhase(
        field_datatype_map={f: "string" for f in row.keys()},
        issues=issues,
        dataset=dataset,
        valid_category_values=valid_category_values or {},
    )

    # Pass-through normalisation to avoid external datatype behaviour affecting this test
    def passthrough(fieldname, value):
        return "" if value is None else str(value)

    phase.harmonise_field = passthrough

    out = list(phase.process([make_block(row)]))
    return issues, out


def test_known_dataset_enforces_mandatories_and_geometry_point():
    """Known dataset: enforce dataset-specific fields and geometry/point rule."""
    row = {
        "reference": "CA-001",
        "geometry": "",
        "point": "",
        "name": "",
    }
    issues, out = run_phase("conservation-area", row)

    missing_name = [
        i
        for i in issues.logged
        if i["field"] == "name" and i["issue_type"] == "missing value"
    ]
    assert missing_name, "Expected a missing value issue for 'name'"

    geo_or_point_missing = [
        i for i in issues.logged if i["field"] in ("geometry", "point") and i["issue_type"] == "missing value"
    ]

    assert (
        geo_or_point_missing
    ), "Expected at least one missing value issue for geometry/point"

    assert out and "row" in out[0]


def test_unknown_dataset_only_reference_required_when_present_no_issues():
    """Unknown dataset: only reference required; others should not log issues."""
    row = {
        "reference": "X-123",
        "name": "",
        "geometry": "",
        "point": "",
    }
    issues, _ = run_phase("not-in-spec", row)
    assert (
        issues.logged == []
    ), "No issues should be logged when reference is present for unknown datasets"


def test_unknown_dataset_reference_missing_triggers_issue_only_for_reference():
    """Unknown dataset: empty reference logs exactly one issue, nothing else."""
    row = {
        "reference": "",
        "name": "",
        "geometry": "",
        "point": "",
    }
    issues, _ = run_phase("some-unknown-dataset", row)

    ref_issues = [
        i
        for i in issues.logged
        if i["field"] == "reference" and i["issue_type"] == "missing value"
    ]
    assert len(ref_issues) == 1, "Expected exactly one 'reference' missing issue"

    others = [i for i in issues.logged if i["field"] != "reference"]
    assert not others, "No other fields should be enforced for unknown datasets"


def test_unknown_dataset_missing_reference_key_now_logs_issue():
    """Unknown dataset with no 'reference' key should log a reference issue."""
    row = {
        "name": "anything",
        "geometry": "",
        "point": "",
    }
    issues, _ = run_phase("unknown-ds", row)
    assert any(
        i["field"] == "reference" and i["issue_type"] == "missing value"
        for i in issues.logged
    ), "Expected reference issue when 'reference' field is absent"


def test_known_dataset_missing_reference_logs_once():
    """Known dataset missing reference should log exactly one reference issue."""
    row = {
        "reference": "",
        "geometry": "",
        "name": "",
    }
    issues, _ = run_phase("conservation-area", row)
    ref_issues = [
        i
        for i in issues.logged
        if i["field"] == "reference" and i["issue_type"] == "missing value"
    ]
    assert (
        len(ref_issues) == 1
    ), "Expected exactly one reference-missing issue for known dataset"


def test_brownfield_land_is_exempt_from_global_reference_check():
    """brownfield-land is exempt: missing/absent 'reference' should NOT be logged."""
    row = {
        "OrganisationURI": "org:1",
        "SiteReference": "SR-1",
        "SiteNameAddress": "Foo",
        "GeoX": "1.0",
        "GeoY": "2.0",
    }
    issues, _ = run_phase("brownfield-land", row)
    assert not any(
        i["field"] == "reference" for i in issues.logged
    ), "brownfield-land should be exempt from global reference enforcement"
