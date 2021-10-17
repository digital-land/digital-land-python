import logging
import re
import time
import datetime

import requests
from tenacity import retry, stop_after_attempt


class Slugger:
    def __init__(self, prefix, key_field, scope_field=None):
        self.prefix = prefix
        self.key_field = key_field
        self.scope_field = scope_field

    def _generate_slug(self, row):
        return self.generate_slug(self.prefix, self.key_field, row, self.scope_field)

    bad_chars = re.compile(r"[^A-Za-z0-9]")

    @staticmethod
    def generate_slug(prefix, key_field, row, scope_field=None):
        """
        Helper method to provide slug without a Slugger instance
        """
        for field in [scope_field, key_field]:
            if not field:
                continue
            if field not in row or not row[field]:
                return None

        scope = None
        if scope_field:
            scope = row[scope_field].replace(":", "/")

        key_parts = row[key_field].split(":")
        if len(key_parts) == 2 and scope:
            # do not include first part of curie if we have scope
            key_parts.pop(0)

        key_parts[-1] = Slugger.bad_chars.sub("-", key_parts[-1])
        key = "/".join(key_parts)

        return "/" + "/".join(filter(None, [prefix, scope, key]))

    @staticmethod
    def send_to_cloud_log(slug, statement, etime):
        requests.post(
            "https://wpjddrq339.execute-api.eu-west-2.amazonaws.com/log",
            json={
                "slug": slug,
                "log": statement,
                "date": datetime.datetime.utcfromtimestamp(etime).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
            },
        )

    @staticmethod
    @retry(stop=stop_after_attempt(5))
    def get_entity_from_slug(slug):
        if not slug:
            return None

        start_time = time.time()
        base_url = "https://www.digital-land.info/search?slug={}"
        try:
            response = requests.get(
                base_url.format(slug),
                headers={"User-Agent": "Digital Land"},
                timeout=120,
            )
            if response.status_code == 404:
                logging.warning(
                    "failed to lookup entity for %s [%s]", slug, response.status_code
                )
                Slugger.send_to_cloud_log(slug, "404 entity not found", start_time)
                return None

            entity_number = int(response.text)
        except Exception as e:
            logging.warning(
                "%s: failed to lookup entity for %s", type(e).__name__, slug
            )
            logging.warning(e)
            Slugger.send_to_cloud_log(slug, "entity exception : " + str(e), start_time)
            raise

        logging.debug(
            "entity lookup completed in %.3fsseconds", time.time() - start_time
        )

        return entity_number

    def slug(self, reader):
        for stream_data in reader:
            row = stream_data["row"]

            if not row.get("slug", ""):
                row["slug"] = self._generate_slug(row)

            if not row.get("entity", ""):
                row["entity"] = self.get_entity_from_slug(row["slug"])

            yield stream_data
