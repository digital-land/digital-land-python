import logging
import time
import datetime
import os

import requests
import urllib.parse
import mysql.connector
from pathlib import Path
from abc import ABC, abstractmethod


class EntityConnector(ABC):
    def __init__(self):
        self.log_url = os.environ.get("ENTITY_LOOKUP_LOG_URL")

    @abstractmethod
    def get_entity_from_slug(self, slug):
        pass

    def close(self):
        pass

    def log_entity(self, slug, statement):
        if self.log_url:
            requests.post(
                self.log_url,
                json={
                    "slug": slug,
                    "log": statement,
                    "date": datetime.datetime.utcfromtimestamp(time.time()).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                },
            )


class APIEntityConnector(EntityConnector):
    def __init__(self):
        self.api_url = "https://www.digital-land.info/search?slug={}"
        self.session = requests.session()
        self.session.headers = {"User-Agent": "Digital Land"}

    def close(self):
        self.session.close()

    def get_entity_from_slug(self, slug):
        entity_number = None
        response = self.session.get(
            self.api_url.format(urllib.parse.quote(slug)),
            timeout=120,
        )
        response.raise_for_status()
        entity_number = int(response.text)

        return entity_number


class DatabaseEntityConnector(EntityConnector):
    def __init__(self):
        self.config = {
            "host": os.environ["ENTITY_LOOKUP_DB_HOST"],
            "port": 3306,
            "user": os.environ["ENTITY_LOOKUP_DB_USER"],
            "database": "test",
            "password": os.environ["ENTITY_LOOKUP_DB_PASSWORD"],
            "ssl_ca": str(
                Path(__file__).parent.parent.resolve() / ".cert/entity_db_ssl.pem"
            ),
        }

        self.db_conn = mysql.connector.connect(**self.config)
        self.db_cursor = self.db_conn.cursor(dictionary=True)

    def close(self):
        self.db_cursor.close()
        self.db_conn.close()

    def get_entity_from_slug(self, slug):
        self.db_cursor.execute("SELECT entity FROM slug WHERE slug.slug=%s", (slug,))
        result = self.db_cursor.fetchall()

        if len(result) > 1:
            logging.warning("Multiple entities returned for %s", slug)
            return None

        if len(result) == 0:
            logging.warning(f"failed to lookup entity for {slug}")
            self.log_entity(slug, f"failed to lookup entity for {slug}")
            return None

        result = result[0]

        return result["entity"] if "entity" in result else None


class EntityLookup:
    def __init__(self):
        self.connector = (
            DatabaseEntityConnector()
            if os.environ.get("ENTITY_LOOKUP_DB_HOST")
            else APIEntityConnector()
        )

    def close(self):
        self.connector.close()

    def lookup(self, reader):
        for stream_data in reader:
            row = stream_data["row"]
            if "slug" in row:
                entity_number = None
                try:
                    entity_number = self.connector.get_entity_from_slug(row["slug"])
                except Exception as e:
                    logging.warning(
                        "%s: failed to lookup entity for %s",
                        type(e).__name__,
                        row["slug"],
                    )
                    logging.warning(e)
                row["entity"] = entity_number
            yield stream_data
