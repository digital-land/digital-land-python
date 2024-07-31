import csv
import logging
import os
import pytest
from digital_land.check import duplicate_reference_check
from digital_land.log import IssueLog

transformed_headers = [
    "end-date",
    "entity",
    "entry-date",
    "entry-number",
    "fact",
    "field",
    "reference-entity",
    "resource",
    "start-date",
    "value",
]


def test_duplicate_reference_check(tmp_path):
    transformed_rows = [
        {
            "entity": 7010002598,
            "entry-date": "2024-07-19",
            "entry-number": 1,
            "field": "name",
            "value": "name1",
        },
        {
            "entity": 7010002598,
            "entry-date": "2024-07-19",
            "entry-number": 1,
            "field": "reference",
            "value": "ref1",
        },
        {
            "entity": 7010002598,
            "entry-date": "2024-07-19",
            "entry-number": 2,
            "field": "reference",
            "value": "ref1",
        },
    ]
    transformed_csv_path = os.path.join(tmp_path, "transformed.csv")
    with open(transformed_csv_path, "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=transformed_headers)
        dictwriter.writeheader()
        dictwriter.writerows(transformed_rows)

    issues = IssueLog()
    issues = duplicate_reference_check(issues=issues, csv_path=transformed_csv_path)
    assert len(issues.rows) == 2
    assert issues.rows[0]["value"] == "ref1"
    assert issues.rows[1]["value"] == "ref1"
    assert issues.rows[0]["entry-number"] == 1
    assert issues.rows[1]["entry-number"] == 2

    assert issues.rows[0]["field"] == "reference"
    assert issues.rows[0]["issue-type"] == "reference values are not unique"


@pytest.mark.parametrize(
    "csv_path, transformed_rows",
    [
        (
            "no_duplicate.csv",
            [
                {
                    "entity": 7010002598,
                    "entry-date": "2024-07-19",
                    "entry-number": 1,
                    "field": "reference",
                    "value": "ref1",
                },
                {
                    "entity": 7010002599,
                    "entry-date": "2024-07-20",
                    "entry-number": 2,
                    "field": "reference",
                    "value": "ref2",
                },
                {
                    "entity": 7010002600,
                    "entry-date": "2024-07-20",
                    "entry-number": 3,
                    "field": "reference",
                    "value": "ref3",
                },
            ],
        ),
        (
            "different_entry_date.csv",
            [
                {
                    "entity": 7010002598,
                    "entry-date": "2024-07-19",
                    "entry-number": 1,
                    "field": "reference",
                    "value": "ref1",
                },
                {
                    "entity": 7010002599,
                    "entry-date": "2024-07-20",
                    "entry-number": 2,
                    "field": "reference",
                    "value": "ref1",
                },
            ],
        ),
        (
            "no_references.csv",
            [
                {
                    "entity": 7010002598,
                    "entry-date": "2024-07-19",
                    "entry-number": 1,
                    "field": "name",
                    "value": "name1",
                },
                {
                    "entity": 7010002598,
                    "entry-date": "2024-07-19",
                    "entry-number": 1,
                    "field": "name",
                    "value": "name2",
                },
            ],
        ),
        ("headers_only.csv", []),
    ],
)
def test_duplicate_reference_no_issues(tmp_path, csv_path, transformed_rows):
    transformed_csv_path = os.path.join(tmp_path, csv_path)
    with open(transformed_csv_path, "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=transformed_headers)
        dictwriter.writeheader()
        dictwriter.writerows(transformed_rows)

    issues = IssueLog()
    issues = duplicate_reference_check(issues=issues, csv_path=transformed_csv_path)
    assert len(issues.rows) == 0


def test_duplicate_reference_check_empty_file(tmp_path, caplog):
    transformed_csv_path = os.path.join(tmp_path, "empty.csv")
    open(transformed_csv_path, "a").close()

    issues = IssueLog()
    with caplog.at_level(logging.ERROR):
        issues = duplicate_reference_check(issues=issues, csv_path=transformed_csv_path)
    assert len(issues.rows) == 0
    assert "Failed" in caplog.text
