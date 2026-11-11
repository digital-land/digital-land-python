"""
Checks for URLs that the Collector fetches.

Endpoint URLs come from outside the platform, so a fetch must not be able to
read local files (file:) or reach internal addresses such as cloud metadata and
credential endpoints.

check_fetch_url is a static check that needs no network access. The addresses
actually connected to are checked by digital_land.utils.safe_fetch, which also
covers hostnames that resolve to internal addresses and redirects.
"""

import ipaddress
import socket
from urllib.parse import urlsplit

ALLOWED_SCHEMES = {"http", "https"}
_BLOCKED_HOSTNAMES = {"localhost"}
# Characters that URL parsers disagree on, which can make the host we check
# differ from the host that is actually contacted.
_DISALLOWED_CHARACTERS = set("\\") | {chr(c) for c in range(0x20)} | {"\x7f"}


class UnsafeUrlError(ValueError):
    """The URL must not be fetched."""


def parse_ip(host):
    """
    Return the address if host is an IP literal, else None.

    Also accepts the legacy IPv4 forms that resolvers understand, such as
    "2852039166", "0xa9fea9fe" or "169.254.43518", which all mean
    169.254.169.254.
    """
    try:
        return ipaddress.ip_address(host)
    except ValueError:
        pass
    try:
        return ipaddress.IPv4Address(socket.inet_aton(host))
    except OSError:
        return None


def is_public_ip(ip):
    """True if ip is a publicly routable address."""
    if ip.version == 6 and ip.ipv4_mapped is not None:
        ip = ip.ipv4_mapped
    return ip.is_global


def check_fetch_url(url):
    """
    Raise UnsafeUrlError unless url is an http(s) URL whose host is not an
    internal address. Returns the url unchanged.
    """
    if not isinstance(url, str) or not url.strip():
        raise UnsafeUrlError("A URL is required")
    if any(character in _DISALLOWED_CHARACTERS for character in url):
        raise UnsafeUrlError("URL contains characters that are not allowed")

    try:
        # requests ignores leading and trailing spaces, so check the same URL it fetches
        parts = urlsplit(url.strip())
        host = parts.hostname
        parts.port  # raises ValueError for an invalid port
    except ValueError as e:
        raise UnsafeUrlError("URL could not be parsed") from e

    if parts.scheme.lower() not in ALLOWED_SCHEMES:
        raise UnsafeUrlError("Only http and https URLs are allowed")
    if not host:
        raise UnsafeUrlError("URL must include a host")

    host = host.rstrip(".").lower()
    if host in _BLOCKED_HOSTNAMES or host.endswith(".localhost"):
        raise UnsafeUrlError("URL must not point at this machine")

    ip = parse_ip(host)
    if ip is not None and not is_public_ip(ip):
        raise UnsafeUrlError("URL must not point at an internal address")

    return url
