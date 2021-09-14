import logging
import urllib.parse

import requests

logger = logging.getLogger(__name__)

url_base = "http://www.digital-land.info/search/"


def lookup_by_slug(slug: str) -> int:
    url = f"{url_base}?slug={urllib.parse.quote(slug)}"
    logger.info("looking up entity at %s", url)
    resp = requests.get(url)
    if resp.status_code != 200:
        raise ValueError(f"slug {slug} not found")

    entity = resp.json()
    if not isinstance(entity, int):
        raise ValueError(f"error looking up slug, expected int")

    return entity
