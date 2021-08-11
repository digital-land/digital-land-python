import json
import logging
import sqlite3
import time
from abc import ABC, abstractmethod

import requests
from datasette_builder import canned_query

logger = logging.getLogger(__name__)


class ViewModel(ABC):
    @abstractmethod
    def get_references_by_id(self, table, id):
        pass

    @abstractmethod
    def get_entity_by_id(self, table, id):
        pass

    @abstractmethod
    def get_id_by_slug(self, slug):
        pass


class ViewModelLocalQuery(ViewModel):
    valid_tables = {"category", "geography"}

    def __init__(self, path="dataset/view_model.sqlite3"):
        self.path = path
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        self.queries = canned_query.generate_model_canned_queries(pagination=False)

    def dicts_from(self, cursor):
        return [dict(row) for row in cursor.fetchall()]

    def get_id(self, table, value):
        qry = self.queries[f"get_{table}_id"]["sql"]
        cur = self.conn.cursor()
        cur.execute(qry, (value,))
        return self.dicts_from(cur)

    def get_references_by_id(self, table, id):
        qry = self.queries[f"get_{table}_references"]["sql"]
        cur = self.conn.cursor()
        cur.execute(qry, (id,))
        return self.dicts_from(cur)


class ViewModelJsonQuery(ViewModel):
    def __init__(self, url_base="https://datasette-demo.digital-land.info/view_model/"):
        self.url_base = url_base

    def get_id_by_slug(self, value):
        url = f"{self.url_base}slug.json"
        params = [
            "_shape=objects",
            f"slug={requests.utils.quote(value)}",
        ]

        url = f"{url}?{'&'.join(params)}"
        results = self.paginate_simple(url)
        assert len(results) == 1
        return results[0]["id"]

    def get_entity_by_id(self, table, value):
        url = f"{self.url_base}{table}.json"
        params = [
            "_shape=objects",
            f"slug_id={requests.utils.quote(str(value))}",
        ]

        url = f"{url}?{'&'.join(params)}"
        results = self.paginate_simple(url)
        assert len(results) == 1
        return results[0]

    def get_id(self, table, value):
        url = f"{self.url_base}get_{table}_id.json"
        params = [
            "_shape=objects",
            f"{requests.utils.quote(table)}={requests.utils.quote(value)}",
        ]

        url = f"{url}?{'&'.join(params)}"
        return self.paginate(url)

    def get_references_by_id(self, table, id):
        url = f"{self.url_base}get_{table}_references.json"
        params = ["_shape=objects", f"{requests.utils.quote(table)}={id}"]

        url = f"{url}?{'&'.join(params)}"
        return self.paginate(url)

    def select(self, table, exact={}, joins=[], label=None, sort=None):
        url = f"{self.url_base}{table}.json"
        params = ["_shape=objects"]

        if label:
            params.append(f"_label={label}")

        if sort:
            params.append(f"_sort={sort}")

        for column, value in exact.items():
            params.append(f"{column}__exact={requests.utils.quote(value)}")

        for clause in joins:
            params.append(f"_through={requests.utils.quote(json.dumps(clause))}")

        param_string = "&".join(params)
        if param_string:
            url = f"{url}?{param_string}"

        return self.paginate(url)

    def get(self, url):
        try:
            response = requests.get(url)
        except ConnectionRefusedError:
            raise ConnectionError("failed to connect to view model api at %s" % url)
        return response

    def paginate_simple(self, url):
        items = []
        while url:
            start_time = time.time()
            response = self.get(url)
            logger.info("request time: %.2fs, %s", time.time() - start_time, url)
            try:
                url = response.links.get("next").get("url")
            except AttributeError:
                url = None
            items.extend(response.json()["rows"])
        return items

    def paginate(self, url):
        limit = -1
        more = True
        while more:
            paginated_url = url + f"&gid={limit}"
            start_time = time.time()
            response = self.get(paginated_url)
            logger.info(
                "request time: %.2fs, %s", time.time() - start_time, paginated_url
            )
            try:
                data = response.json()
            except Exception as e:
                logger.error(
                    "json not found in response (url: %s):\n%s",
                    paginated_url,
                    response.content,
                )
                raise e

            if "rows" not in data:
                logger.warning("url: %s", paginated_url)
                raise ValueError('no "rows" found in response:\n%s', data)

            if "expanded_columns" in data:
                row_iter = self.expand_columns(data)
            else:
                row_iter = data["rows"]

            if "truncated" in data and data["truncated"]:
                more = True
                limit = data["rows"][-1]["gid"]
            else:
                more = False

            yield from row_iter

    def expand_columns(self, data):
        col_map = {}
        for config, dest_col in data["expandable_columns"]:
            if config["column"] in data["expanded_columns"]:
                if dest_col in data["columns"]:
                    raise ValueError(f"name clash trying to expand {dest_col} label")
                col_map[config["column"]] = dest_col

        for row in data["rows"]:
            for src, dest in col_map.items():
                row[dest] = row[src]["label"]
                row[src] = row[src]["value"]
            yield row
