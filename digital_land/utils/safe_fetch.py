"""
Stop URL fetches from reaching internal addresses.

digital_land.utils.url_safety.check_fetch_url rejects bad URLs up front, but a
public hostname can still resolve to an internal address, a public URL can
redirect to an internal one, and DNS answers can change between a check and the
connection.

untrusted_fetches() closes those gaps for everything that goes through urllib3
(requests, and so the Collector and the arcgis plugin). While it is active on
the current thread, every new connection resolves the host, refuses non-public
addresses, and connects to the address it checked. Connections made outside the
context manager are unaffected.

The paged WFS plugin fetches with ogr2ogr (GDAL) in a subprocess, which this
cannot see, so check_host_resolves_publicly is also called before fetching.
"""

import contextlib
import ipaddress
import socket
import threading
from urllib.parse import urlsplit

import urllib3.util.connection as urllib3_connection

from digital_land.utils.url_safety import UnsafeUrlError, is_public_ip

_state = threading.local()
_original_create_connection = urllib3_connection.create_connection


class BlockedAddressError(OSError):
    """A fetch tried to connect to a non-public address."""


def _public_addresses(host, port):
    """Resolve host and return its addresses, refusing any non-public one."""
    addresses = socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM)
    if not addresses:
        raise BlockedAddressError(f"{host} did not resolve")
    for *_, sockaddr in addresses:
        ip = ipaddress.ip_address(sockaddr[0].split("%", 1)[0])
        if not is_public_ip(ip):
            raise BlockedAddressError(f"{host} resolves to a non-public address")
    return [sockaddr[0] for *_, sockaddr in addresses]


def _guarded_create_connection(address, *args, **kwargs):
    if not getattr(_state, "active", False):
        return _original_create_connection(address, *args, **kwargs)

    host, port = address
    host = host.strip("[]")
    last_error = None
    # Connect to the address that was checked, so a second DNS lookup can't
    # return something different. TLS still verifies against the hostname,
    # which urllib3 passes separately when wrapping the socket.
    for ip in dict.fromkeys(_public_addresses(host, port)):
        try:
            return _original_create_connection((ip, port), *args, **kwargs)
        except OSError as e:
            last_error = e
    raise last_error


def _install():
    if urllib3_connection.create_connection is not _guarded_create_connection:
        urllib3_connection.create_connection = _guarded_create_connection


_install()


@contextlib.contextmanager
def untrusted_fetches():
    """Refuse connections to non-public addresses on this thread while active."""
    previous = getattr(_state, "active", False)
    _state.active = True
    try:
        yield
    finally:
        _state.active = previous


def check_host_resolves_publicly(url):
    """Raise UnsafeUrlError unless every address the URL's host resolves to is public."""
    parts = urlsplit(url.strip())
    default_port = 80 if parts.scheme.lower() == "http" else 443
    try:
        _public_addresses(parts.hostname, parts.port or default_port)
    except (BlockedAddressError, OSError) as e:
        raise UnsafeUrlError(f"URL host is not allowed: {e}") from e
