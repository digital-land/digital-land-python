import pathlib
import sys
import types

import pytest

# --- Make repo root importable ---
# tests/unit/phase/test_harmonise.py -> repo root
repo_root = pathlib.Path(__file__).resolve().parents[2]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

# --- Lightweight stubs to avoid heavy deps during import ---
# 1) Stub digital_land.datatype.factory (avoids validators/requests/etc.)
dl_dt_factory = types.ModuleType("digital_land.datatype.factory")


def datatype_factory(datatype_name: str):
    class _DT:
        def normalise(self, v, issues=None):
            return "" if v is None else str(v)

    return _DT()


dl_dt_factory.datatype_factory = datatype_factory
sys.modules["digital_land.datatype.factory"] = dl_dt_factory

# 2) Stub digital_land.datatype.point.PointDataType
dl_dt_point = types.ModuleType("digital_land.datatype.point")


class PointDataType:
    def normalise(self, value, issues=None):
        try:
            x, y = value
            x = float(x)
            y = float(y)
            return f"POINT ({x} {y})"
        except Exception:
            return ""


dl_dt_point.PointDataType = PointDataType
sys.modules["digital_land.datatype.point"] = dl_dt_point

# 3) If shapely isn't installed, stub shapely.wkt.loads
try:
    import shapely.wkt  # type: ignore  # noqa: F401
except Exception:
    shapely = types.ModuleType("shapely")
    shapely_wkt = types.ModuleType("shapely.wkt")

    class _Geom:
        def __init__(self, x, y):
            self.coords = [(x, y)]

    def loads(s):
        inner = s[s.find("(") + 1 : s.find(")")]
        x, y = map(float, inner.split())
        return _Geom(x, y)

    shapely_wkt.loads = loads
    sys.modules["shapely"] = shapely
    sys.modules["shapely.wkt"] = shapely_wkt

# --- Import the SUT AFTER stubs are in place ---
from digital_land.phase.harmonise import HarmonisePhase  # noqa: E402


class FakeIssues:
    def __init__(self):
        self.logged = []
        # attributes set by HarmonisePhase
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

    # Stub out harmonise_field to be pass-through (avoids real datatypes)
    def passthrough(fieldname, value):
        return "" if value is None else str(value)

    phase.harmonise_field = passthrough

    out = list(phase.process([make_block(row)]))
    return issues, out


def test_known_dataset_enforces_mandatories_and_geometry_point():
    """
    Known dataset ('conservation-area') should:
      - enforce dataset-specific mandatory fields: ['reference', 'geometry', 'name']
      - enforce geometry/point co-constraint (one of them must be present if either
        field is given)
    """
    row = {
        "reference": "CA-001",
        "geometry": "",
        "point": "",
        "name": "",
    }
    issues, out = run_phase("conservation-area", row)

    # Mandatory 'name' missing
    missing_name = [
        i
        for i in issues.logged
        if i["field"] == "name" and i["issue_type"] == "missing value"
    ]
    assert missing_name, "Expected a missing value issue for 'name'"

    # Geometry/point co-constraint
    geo_or_point_missing = [
        i
        for i in issues.logged
        if i["field"] in ("geometry", "point")
        and i["issue_type"] == "missing value"
    ]
    assert (
        geo_or_point_missing
    ), "Expected at least one missing value issue for geometry/point"

    assert out and "row" in out[0]


def test_unknown_dataset_only_reference_required_when_present_no_issues():
    """
    Unknown dataset should only require 'reference'. If reference is present,
    missing geometry/point or other fields should NOT create issues.
    """
    row = {
        "reference": "X-123",
        "name": "",
        "geometry": "",
        "point": "",
    }
    issues, _ = run_phase("not-in-spec", row)
    assert (
        issues.logged == []
    ), "No issues should be logged for an unknown dataset when 'reference' is present"


def test_unknown_dataset_reference_missing_triggers_issue_only_for_reference():
    """
    Unknown dataset with missing 'reference' should log exactly one issue for
    'reference' and nothing else (no geometry/point checks).
    """
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


# --- New tests for global 'reference' + exemption behaviour --- #
def test_unknown_dataset_missing_reference_key_now_logs_issue():
    """Unknown dataset with no 'reference' key should log a reference-missing issue (global rule)."""
    row = {
        "name": "anything",
        "geometry": "",
        "point": "",
    }
    issues, _ = run_phase("unknown-ds", row)
    assert any(
        i["field"] == "reference" and i["issue_type"] == "missing value"
        for i in issues.logged
    ), "Expected global reference issue when 'reference' field is absent"


def test_known_dataset_missing_reference_logs_once():
    """Known dataset missing reference should log exactly one reference issue (global), not duplicate."""
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
        # No 'reference' at all
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
