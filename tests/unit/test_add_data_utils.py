import pytest
import pandas as pd

from digital_land.commands import is_url_valid
from digital_land.utils.add_data_utils import (
    get_user_response,
    is_date_valid,
    get_provision_entities_from_duckdb,
    normalise_json,
)


def test_is_url_valid():
    isValid, error = is_url_valid("https://www.google.com", "URL")

    assert isValid
    assert error == ""


@pytest.mark.parametrize(
    "url, error_message",
    [
        (
            "",
            "The URL must be populated",
        ),
        (
            "www.google.com",
            "The URL must start with 'http://' or 'https://'",
        ),
        (
            "https:///query=?a=1&b=2",
            "The URL must have a domain",
        ),
        (
            "https://google",
            "The URL must have a valid domain with a top-level domain (e.g., '.gov.uk', '.com')",
        ),
    ],
)
def test_is_url_valid_error(url, error_message):
    isValid, error = is_url_valid(url, "URL")

    assert not isValid
    assert error == error_message


def test_is_date_valid():
    isValid, error = is_date_valid("2000-12-25", "date")

    assert isValid
    assert error == ""


@pytest.mark.parametrize(
    "date, error_message",
    [
        (
            "",
            "Date is blank",
        ),
        (
            "25-12-2000",
            "date 25-12-2000 must be format YYYY-MM-DD",
        ),
        (
            "9999-12-25",
            "The date 9999-12-25 cannot be in the future",
        ),
    ],
)
def test_is_date_valid_error(date, error_message):
    isValid, error = is_date_valid(date, "date")

    assert not isValid
    assert error == error_message


def test_get_user_response(monkeypatch):

    # Mock in user input
    monkeypatch.setattr("builtins.input", lambda _: "yes")

    result = get_user_response("message")

    assert result


def test_get_user_response_fail(monkeypatch):

    # Mock in user input
    monkeypatch.setattr("builtins.input", lambda _: "not yes")

    result = get_user_response("message")

    assert not result


def test_normalise_json():
    json_string = '{"secondproperty": "secondvalue", "firstproperty": "firstvalue"}'

    sorted_json_string = normalise_json(json_string)

    # ensure json is sorted
    assert isinstance(sorted_json_string, str)
    assert sorted_json_string.find("firstproperty") < sorted_json_string.find(
        "secondproperty"
    )


def test_get_provision_entities_from_duckdb(tmp_path):

    csv_file = tmp_path / "lookup.csv"
    df = pd.DataFrame(
        {
            "prefix": ["tree", "tree"],
            "organisation": ["organisation", "organisation"],
            "reference": ["ref,1", "ref,2"],
            "entity": ["10", "11"],
        }
    )
    df.to_csv(csv_file, index=False)

    pipeline = "tree"
    endpoint_resource_info = {"organisation": "organisation"}

    result = get_provision_entities_from_duckdb(
        csv_file, pipeline, endpoint_resource_info
    )

    assert list(result) == [10, 11]
