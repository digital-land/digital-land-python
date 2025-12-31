"""
Module to contain the primary Classes and functions for the configuration package
The config class is primarily how to access all the different configuration classes.

"""

import sqlite3
import urllib
from pathlib import Path

from digital_land.package.sqlite import SqlitePackage
from digital_land.package.package import Specification
from digital_land.phase.map import normalise


class Config(SqlitePackage):
    """
    A class which represents configuration to be used to control collection and pipeline processes.
    """

    def __init__(
        self,
        path: Path,
        specification: Specification,
        tables: dict = None,
        indexes=None,
    ):
        self.path = Path(path)
        self.specification = specification  # TODO to remove?
        self.tables = tables or {
            "entity-organisation": "pipeline",
            "expect": "pipeline",
            "column": "pipeline",
        }

        self.indexes = {
            "entity-organisation": ["organisation", "entity-minimum", "entity-maximum"],
        }

        self._spatialite = None

    def create(self):
        self.create_database()
        self.create_indexes()
        self.disconnect()

    # TODO This provides access to the entity organisations. Should the config have an object for each File
    def get_entity_organisation(self, entity):
        self.connect()
        self.create_cursor()
        self.cursor.execute(
            f"select organisation from entity_organisation where entity_minimum <= {entity} and entity_maximum >= {entity}"
        )
        row = self.cursor.fetchone()

        result = row[0] if row else None
        self.disconnect()

        return result

    def get_pipeline_columns(
        self, dataset: str, resource: str = "", endpoints=None
    ) -> dict:
        """
        Return {normalised_source_column: target_field} for a dataset/resource/endpoints.
        Precedence: general < endpoints (in order) < resource.
        """
        endpoints = endpoints or []

        self.connect()
        self.connection.row_factory = sqlite3.Row
        self.create_cursor()

        def fetch(where_sql, params):
            self.cursor.execute(
                f"""
                select column, field
                from column
                where (dataset = '' or dataset = ?)
                  and {where_sql}
                """,
                params,
            )
            return [dict(r) for r in self.cursor.fetchall()]

        # general rows (no endpoint/resource)
        rows = fetch(
            "(endpoint = '' or endpoint is null) and (resource = '' or resource is null)",
            [dataset],
        )

        # endpoint-specific rows
        for endpoint in endpoints:
            rows += fetch("endpoint = ?", [dataset, endpoint])

        # resource-specific rows
        if resource:
            rows += fetch("resource = ?", [dataset, resource])

        self.disconnect()

        # merge into final mapping
        columns = {}
        for r in rows:
            if not r.get("column"):
                continue
            columns[normalise(r["column"])] = r.get("field", "")
        return columns

    def get_expectation_rules(self, dataset):
        self.connect()
        self.connection.row_factory = sqlite3.Row
        self.create_cursor()
        self.cursor.execute(
            f"""
                select *
                from expect
                where instr(';' || datasets || ';', ';{dataset};') > 0;
            """
        )
        rows = self.cursor.fetchall()

        results = [dict(row) for row in rows]
        self.disconnect()
        return results

    def load(self, tables=None):
        tables = tables or self.tables
        self.connect()
        self.create_cursor()
        self.drop_indexes()
        super().load(tables)
        self.create_indexes()
        self.disconnect()

    @staticmethod
    def download_pipeline_files(path, collection):
        """
        Download the pipeline configuration files from the data collection cdn repository.
        Args:
            path (Path): The path to download the files to.
            collection (str): The collection name to download the files for.
        """
        path = Path(path)
        source_url = f"https://files.planning.data.gov.uk/config/pipeline/{collection}"
        pipeline_csvs = [
            "column.csv",
            "combine.csv",
            "concat.csv",
            "convert.csv",
            "default.csv",
            "default-value.csv",
            "filter.csv",
            "lookup.csv",
            "old-entity.csv",
            "patch.csv",
            "skip.csv",
            "transform.csv",
            "entity-organisation.csv",
            "expect.csv",
        ]
        for pipeline_csv in pipeline_csvs:
            urllib.request.urlretrieve(
                f"{source_url}/{pipeline_csv}",
                path / pipeline_csv,
            )
