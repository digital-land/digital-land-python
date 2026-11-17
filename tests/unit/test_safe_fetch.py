import socket

import pytest
import urllib3.util.connection as urllib3_connection

from digital_land.utils import safe_fetch
from digital_land.utils.safe_fetch import (
    BlockedAddressError,
    check_host_resolves_publicly,
    untrusted_fetches,
)
from digital_land.utils.url_safety import UnsafeUrlError


def _addrinfo(*ips):
    return [
        (
            socket.AF_INET6 if ":" in ip else socket.AF_INET,
            socket.SOCK_STREAM,
            6,
            "",
            (ip, 443),
        )
        for ip in ips
    ]


@pytest.fixture
def resolves_to(mocker):
    def _resolves_to(*ips):
        return mocker.patch.object(
            safe_fetch.socket, "getaddrinfo", return_value=_addrinfo(*ips)
        )

    return _resolves_to


@pytest.fixture
def connect(mocker):
    return mocker.patch.object(
        safe_fetch, "_original_create_connection", return_value="socket"
    )


def test_guard_is_installed_on_urllib3():
    assert urllib3_connection.create_connection is safe_fetch._guarded_create_connection


def test_connections_outside_untrusted_fetches_are_not_checked(resolves_to, connect):
    getaddrinfo = resolves_to("169.254.169.254")

    assert urllib3_connection.create_connection(("internal.example", 80)) == "socket"

    getaddrinfo.assert_not_called()
    connect.assert_called_once_with(("internal.example", 80))


def test_connects_to_the_checked_public_address(resolves_to, connect):
    resolves_to("93.184.216.34")

    with untrusted_fetches():
        urllib3_connection.create_connection(("example.com", 443), timeout=5)

    connect.assert_called_once_with(("93.184.216.34", 443), timeout=5)


@pytest.mark.parametrize(
    "ips",
    [
        ("169.254.169.254",),
        ("169.254.170.2",),
        ("127.0.0.1",),
        ("10.1.2.3",),
        ("::1",),
        ("::ffff:169.254.169.254",),
        ("93.184.216.34", "10.1.2.3"),
    ],
)
def test_refuses_hosts_that_resolve_to_non_public_addresses(resolves_to, connect, ips):
    resolves_to(*ips)

    with untrusted_fetches():
        with pytest.raises(BlockedAddressError):
            urllib3_connection.create_connection(("rebind.example", 80))

    connect.assert_not_called()


def test_untrusted_fetches_restores_previous_state(resolves_to, connect):
    resolves_to("169.254.169.254")

    with untrusted_fetches():
        with untrusted_fetches():
            pass
        with pytest.raises(BlockedAddressError):
            urllib3_connection.create_connection(("rebind.example", 80))

    urllib3_connection.create_connection(("rebind.example", 80))
    connect.assert_called_once()


def test_check_host_resolves_publicly_allows_public_host(resolves_to):
    resolves_to("93.184.216.34")

    check_host_resolves_publicly("https://example.com/wfs")


def test_check_host_resolves_publicly_refuses_internal_host(resolves_to):
    resolves_to("169.254.169.254")

    with pytest.raises(UnsafeUrlError):
        check_host_resolves_publicly("https://rebind.example/wfs")


def test_check_host_resolves_publicly_refuses_unresolvable_host(mocker):
    mocker.patch.object(
        safe_fetch.socket, "getaddrinfo", side_effect=socket.gaierror("no such host")
    )

    with pytest.raises(UnsafeUrlError):
        check_host_resolves_publicly("https://does-not-exist.example/wfs")
