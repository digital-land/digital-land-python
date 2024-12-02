import pytest

from digital_land.commands import is_url_valid


def test_is_url_valid():
    isValid, error = is_url_valid("https://www.google.com", "URL")

    assert isValid
    assert error is None


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
