import pytest

from digital_land.commands import is_url_valid
from digital_land.utils.add_data_utils import is_date_valid


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
