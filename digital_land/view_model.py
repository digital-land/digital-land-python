import logging
import sqlite3
import time
import urllib.parse
from abc import ABC, abstractmethod
from typing import List, Optional

import requests
import requests.utils
from datasette_builder import canned_query

logger = logging.getLogger(__name__)


class ViewModel(ABC):
    @abstractmethod
    def get_entity_metadata(self, entity: int) -> Optional[dict]:
        pass

    @abstractmethod
    def get_entity(self, typology: str, entity: int) -> Optional[dict]:
        pass

    @abstractmethod
    def get_references(self, typology: str, entity: int) -> List[dict]:
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

    def get_entity_by_id(self, table, id):
        raise NotImplementedError()

    def get_id_by_slug(self, slug):
        raise NotImplementedError()

    def get_typology_entity_by_slug(self, typology, slug):
        raise NotImplementedError()


class ViewModelJsonQuery(ViewModel):
    def __init__(self, url_base="https://datasette-demo.digital-land.info/view_model/"):
        self.url_base = url_base

    def get_entity_metadata(self, entity: int) -> Optional[dict]:
        url = self.make_url(f"{self.url_base}entity.json", {"entity": entity})
        return self.single_result_from(url)

    def get_entity(self, typology: str, entity: int) -> Optional[dict]:
        if typology not in ["geography", "category", "document", "policy"]:
            raise NotImplementedError(f"not implemented for typology {typology}")

        url = self.make_url(
            f"{self.url_base}typology_{typology}_by_entity.json", {"entity": entity}
        )
        return self.single_result_from(url)

    def get_references(self, typology: str, entity: int) -> List[dict]:
        url = self.make_url(
            f"{self.url_base}get_{typology}_references.json", {typology: entity}
        )
        return self.paginate(url)

    def get_references_by_id(self, table, id):
        url = self.make_url(f"{self.url_base}get_{table}_references.json", {table: id})
        return self.paginate(url)

    def make_url(self, url: str, params: dict) -> str:
        _params = ["_shape=objects"]
        for k, v in params.items():
            k = urllib.parse.quote(k)
            if isinstance(v, str):
                v = urllib.parse.quote(v)
            _params.append(f"{k}={v}")
        url = f"{url}?{'&'.join(_params)}"
        return url

    def single_result_from(self, url: str) -> Optional[dict]:
        results = self.paginate_simple(url)
        if len(results) == 1:
            return results[0]

        if len(results) > 1:
            raise ValueError(f"expected 1 result, got {len(results)}")

        return None

    def get(self, url: str) -> requests.Response:
        try:
            response = requests.get(url)
        except ConnectionRefusedError:
            raise ConnectionError("failed to connect to view model api at %s" % url)
        return response

    def paginate_simple(self, url: str) -> List[dict]:
        items = []
        _url = url
        while _url:
            start_time = time.time()
            response = self.get(_url)
            logger.info("request time: %.2fs, %s", time.time() - start_time, _url)
            try:
                _url = response.links.get("next").get("url")
            except AttributeError:
                _url = None
            items.extend(response.json()["rows"])
        return items

    def paginate(self, url: str) -> List[dict]:
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
