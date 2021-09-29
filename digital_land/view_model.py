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


class JSONQueryHelper:
    @staticmethod
    def paginate_simple(url: str) -> List[dict]:
        items = []
        _url = url
        while _url:
            start_time = time.time()
            response = JSONQueryHelper.get(_url)
            logger.info("request time: %.2fs, %s", time.time() - start_time, _url)
            try:
                _url = response.links.get("next").get("url")
            except AttributeError:
                _url = None
            items.extend(response.json().get("rows", []))
        return items

    @staticmethod
    def paginate(url: str) -> List[dict]:
        limit = -1
        more = True
        while more:
            paginated_url = url + f"&gid={limit}"
            start_time = time.time()
            response = JSONQueryHelper.get(paginated_url)
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
                row_iter = JSONQueryHelper.expand_columns(data)
            else:
                row_iter = data["rows"]

            if "truncated" in data and data["truncated"]:
                more = True
                limit = data["rows"][-1]["gid"]
            else:
                more = False

            yield from row_iter

    @staticmethod
    def expand_columns(data):
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

    @staticmethod
    def make_url(url: str, params: dict) -> str:
        _params = ["_shape=objects"]
        for k, v in params.items():
            k = urllib.parse.quote(k)
            if isinstance(v, str):
                v = urllib.parse.quote(v)
            _params.append(f"{k}={v}")
        url = f"{url}?{'&'.join(_params)}"
        return url

    @staticmethod
    def single_result_from(url: str) -> Optional[dict]:
        results = JSONQueryHelper.paginate_simple(url)
        if len(results) == 1:
            return results[0]

        if len(results) > 1:
            raise ValueError(f"expected 1 result, got {len(results)}")

        return None

    @staticmethod
    def get(url: str) -> requests.Response:
        try:
            response = requests.get(url)
        except ConnectionRefusedError:
            raise ConnectionError("failed to connect to view model api at %s" % url)
        return response


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

    @abstractmethod
    def list_entities(self, typology: str, dataset: str) -> List[dict]:
        pass


class ViewModelLocalQuery(ViewModel):
    valid_tables = {"category", "geography"}

    def __init__(self, path="dataset/view_model.sqlite3"):
        self.path = path
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        self.queries = canned_query.generate_model_canned_queries(pagination=False)

    def get_entity_metadata(self, entity: int) -> Optional[dict]:
        qry = "SELECT * FROM entity WHERE entity = ?"
        cur = self.conn.cursor()
        cur.execute(qry, (entity,))
        result = cur.fetchall()
        assert len(result) == 1, f"expected 1 row got {cur.rowcount}"
        return dict(result[0])

    def get_entity(self, typology: str, entity: int) -> Optional[dict]:
        if typology not in [
            "geography",
            "category",
            "document",
            "policy",
            "organisation",
        ]:
            raise NotImplementedError(f"not implemented for typology {typology}")

        qry = f"SELECT * FROM {typology} WHERE entity = ?"
        cur = self.conn.cursor()
        cur.execute(qry, (entity,))
        result = cur.fetchall()
        assert len(result) == 1, f"expected 1 row got {cur.rowcount}"
        return dict(result[0])

    def get_references(self, typology: str, entity: int) -> List[dict]:
        raise NotImplementedError()

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
    def __init__(self, url_base="https://datasette.digital-land.info/view_model/"):
        self.url_base = url_base

    def get_entity_metadata(self, entity: int) -> Optional[dict]:
        url = JSONQueryHelper.make_url(
            f"{self.url_base}entity.json", {"entity": entity}
        )
        return JSONQueryHelper.single_result_from(url)

    def get_entity(self, typology: str, entity: int) -> Optional[dict]:
        if typology not in [
            "geography",
            "category",
            "document",
            "policy",
            "organisation",
        ]:
            raise NotImplementedError(f"not implemented for typology {typology}")

        url = JSONQueryHelper.make_url(
            f"{self.url_base}typology_{typology}_by_entity.json", {"entity": entity}
        )
        return JSONQueryHelper.single_result_from(url)

    def get_references(self, typology: str, entity: int) -> List[dict]:
        url = JSONQueryHelper.make_url(
            f"{self.url_base}get_{typology}_references.json", {typology: entity}
        )
        return JSONQueryHelper.paginate(url)

    def get_references_by_id(self, table, id):
        url = JSONQueryHelper.make_url(
            f"{self.url_base}get_{table}_references.json", {table: id}
        )
        return JSONQueryHelper.paginate(url)

    def list_entities(self, typology: str, dataset: str) -> List[dict]:
        url = JSONQueryHelper.make_url(
            f"{self.url_base}{typology}.json",
            {
                "_through": '{"table":"entity","column":"dataset","value":"%s"}'
                % dataset,
                "_size": 1000,
            },
        )
        return JSONQueryHelper.paginate_simple(url)


class DigitalLandModelJsonQuery:
    def __init__(self, url_base="https://datasette.digital-land.info/digital-land/"):
        self.url_base = url_base

    def fetch_resource_info(self, resource_hash):
        url = JSONQueryHelper.make_url(
            f"{self.url_base}resource_view_data.json", {"resource": resource_hash}
        )
        return JSONQueryHelper.get(url).json()
