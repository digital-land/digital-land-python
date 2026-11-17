import pytest

from digital_land.utils.url_safety import UnsafeUrlError, check_fetch_url


@pytest.mark.parametrize(
    "url",
    [
        "https://example.com/data.csv",
        "http://example.com",
        "HTTPS://EXAMPLE.COM/data.csv",
        "https://example.com:8443/path?query=1#fragment",
        "https://8.8.8.8/data.csv",
        "https://[2001:4860:4860::8888]/data.csv",
        " https://example.com/data.csv",
    ],
)
def test_check_fetch_url_allows_public_http_urls(url):
    assert check_fetch_url(url) == url


@pytest.mark.parametrize(
    "url",
    [
        # not a web URL
        None,
        "",
        "   ",
        "file:///etc/passwd",
        "file://localhost/etc/passwd",
        "ftp://example.com/data.csv",
        "gopher://example.com/",
        "www.example.com/data.csv",
        "https:///data.csv",
        # this machine
        "http://localhost/",
        "http://LOCALHOST./",
        "http://anything.localhost/",
        "http://127.0.0.1/",
        "http://0.0.0.0/",
        "http://[::1]/",
        # internal and link-local addresses
        "http://10.0.0.1/",
        "http://172.16.0.1/",
        "http://192.168.1.1/",
        "http://100.64.0.1/",
        "http://169.254.169.254/latest/meta-data/",
        "http://169.254.170.2/v2/credentials",
        "http://[fd00::1]/",
        "http://[fe80::1]/",
        "http://[::ffff:169.254.169.254]/",
        # legacy forms of 169.254.169.254
        "http://2852039166/",
        "http://0xa9fea9fe/",
        "http://169.254.43518/",
        # characters URL parsers disagree on
        "http://example.com\\@169.254.169.254/",
        "http://exam\nple.com/",
        "http://example.com\t/",
        # unparseable
        "http://example.com:99999/",
        "http://[not-an-ip/",
    ],
)
def test_check_fetch_url_refuses_unsafe_urls(url):
    with pytest.raises(UnsafeUrlError):
        check_fetch_url(url)
