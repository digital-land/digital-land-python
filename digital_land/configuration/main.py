"""
Module to contain the primary Classes and functions for the configuration package
The config class is primarily how to access all the different configuration classes.

"""

from pathlib import Path

from digital_land.package.sqlite import SqlitePackage
from digital_land.package.package import Specification


class Config(SqlitePackage):
    def __init__(
        self,
        path: Path,
        specification: Specification,
        tables: dict = None,
        indexes=None,
    ):
        # entity_organisation = EntityOrganisation(sqlite_file)
        # will example this as time goes on
        self.path = Path(path)
        self.specification = specification
        self.tables = tables or {
            "entity-organisation": "pipeline",
        }

        self.indexes = {
            "entity-organisation": ["organisation", "entity-minimum", "entity-maximum"],
        }

        self._spatialite = None

    # def load(self,tables):
    #     """
    #     mimicks the same function as in the Sqlite package but allows pipeline directories to be altered so that you can load from multiple places

    #     Arguements:

    #     """
    #     for table in self.tables:
    #         fields = self.fields[table]
    #         path = "%s/%s.csv" % ( or self.tables[table], table)
    #         self.create_cursor()
    #         self.load_table(table, fields, path=path)
    #         self.commit()

    #     for join_table, join in self.join_tables.items():
    #         table = join["table"]
    #         field = join["field"]
    #         fields = [table, field]
    #         path = "%s/%s.csv" % (self.tables[table], table)
    #         self.create_cursor()
    #         self.load_join_table(
    #             join_table,
    #             fields=fields,
    #             split_field=join["split-field"],
    #             field=field,
    #             path=path,
    #         )
    #         self.commit()

    def create(self):
        # indexes are created
        self.create_database()
        self.disconnect()

    # TODO currently this just sits ont he config class I'm wondering
    # if different configurations should be they're own class like an ORM
    def get_entity_organisation(self, entity):
        # TODO need to raise error if more than one row is returned
        # TODO should we replace this with an entity organisation  object that can be queried
        # instead of directly running query
        self.connect()
        self.cursor.execute(
            f"select organisation from entity_organisation where entity_minimum <= {entity} and entity_maximum >= {entity}"
        )
        row = self.cursor.fetchone()

        result = row[0] if row else None
        self.disconnect

        return result

    def load(self, tables=None):
        tables = tables or self.tables
        self.connect()
        super().load(tables)
        self.disconnect()
