from datetime import datetime
import os
import sys
import csv
import sqlite3
import logging
from .package import Package
from decimal import Decimal

logger = logging.getLogger(__name__)


def colname(field):
    if field == "default":
        return "_default"
    return field.replace("-", "_")


def coltype(datatype):
    if datatype == "integer":
        return "INTEGER"
    elif datatype == "json":
        return "JSON"
    else:
        return "TEXT"


# TODO instrument this functionality in SQLAlchemy
class SqlitePackage(Package):
    _spatialite = None

    def spatialite(self, path=None):
        if not path:
            try:
                path = os.environ["SPATIALITE_EXTENSION"]
            except KeyError:
                if sys.platform == "darwin":
                    path = "/usr/local/lib/mod_spatialite.dylib"
                else:
                    path = "/usr/lib/x86_64-linux-gnu/mod_spatialite.so"
        self._spatialite = path

    def connect(self, path):
        self.path = path
        self.connection = sqlite3.connect(path)

        if self._spatialite:
            self.connection.enable_load_extension(True)
            self.connection.load_extension(self._spatialite)
            self.connection.execute("select InitSpatialMetadata(1)")

    def disconnect(self):
        self.connection.close()

    def create_table(
        self, table, fields, key_field=None, field_datatype={}, unique=None
    ):
        self.execute(
            "CREATE TABLE %s (%s%s%s)"
            % (
                colname(table),
                ",\n".join(
                    [
                        "%s %s%s"
                        % (
                            colname(field),
                            coltype(field_datatype[field]),
                            (" PRIMARY KEY" if field == key_field else ""),
                        )
                        for field in fields
                        if not field.endswith("-geom")
                    ]
                ),
                "\n".join(
                    [
                        ", FOREIGN KEY (%s) REFERENCES %s (%s)"
                        % (
                            colname(field),
                            colname(field),
                            colname(field),
                        )
                        for field in fields
                        if field in self.tables and field != table
                    ],
                ),
                "" if not unique else ", UNIQUE(%s)" % (",".join(unique)),
            )
        )

        if self._spatialite:
            if "geometry-geom" in fields:
                self.execute(
                    "SELECT AddGeometryColumn('%s', 'geometry_geom', 4326, 'MULTIPOLYGON', 2);"
                    % (table)
                )
            if "point-geom" in fields:
                self.execute(
                    "SELECT AddGeometryColumn('%s', 'point_geom', 4326, 'POINT', 2);"
                    % (table)
                )

    def create_cursor(self):
        self.cursor = self.connection.cursor()
        self.cursor.execute("PRAGMA synchronous = OFF")
        self.cursor.execute("PRAGMA journal_mode = OFF")

    def commit(self):
        logger.debug("committing ..")

        self.connection.commit()

    def execute(self, cmd):
        logger.debug(cmd)
        try:
            self.cursor.execute(cmd)
        except sqlite3.Error as error:
            logging.error("Exception: %s" % (error.__class__))
            logging.error("Sqlite3 error: %s" % (" ".join(error.args)))
            logging.info("Sqlite3 cmd: %s" % (cmd))
            sys.exit(3)

    def colvalue(self, row, field):
        value = row.get(field, "")
        t = coltype(self.specification.field[field]["datatype"])
        if t == "INTEGER":
            if value == "":
                return "NULL"
            return "%d" % Decimal(value)
        if t == "JSON":
            if value == "{}":
                return "NULL"
        return "'%s'" % value.replace("'", "''")

    def insert(self, table, fields, row):
        fields = [field for field in fields if not field.endswith("-geom")]
        self.execute(
            """
            INSERT OR REPLACE INTO %s(%s)
            VALUES (%s);
            """
            % (
                colname(table),
                ",".join([colname(field) for field in fields]),
                ",".join(["%s" % self.colvalue(row, field) for field in fields]),
            )
        )

    def load_csv(self, path, table, fields):
        logging.info("loading %s from %s" % (table, path))
        for row in csv.DictReader(open(path, newline="")):
            for field in row:
                if row.get(field, None) is None:
                    row[field] = ""
            self.insert(table, fields, row)

    def load_join(self, path, table, fields, split_field=None, field=None):
        logging.info("loading %s from %s" % (table, path))
        for row in csv.DictReader(open(path, newline="")):
            for value in row[split_field].split(";"):
                row[field] = value
                self.insert(table, fields, row)

    def index(self, table, fields, name=None):
        if not name:
            name = colname(table) + "_index"
        logging.info("creating index %s" % (name))
        cols = [colname(field) for field in fields if not field.endswith("-geom")]
        self.execute(
            "CREATE INDEX IF NOT EXISTS %s on %s (%s);"
            % (name, colname(table), ", ".join(cols))
        )

        if self._spatialite:
            logging.info("creating spatial indexes %s" % (name))
            for col in [colname(field) for field in fields if field.endswith("-geom")]:
                self.execute(
                    "SELECT CreateSpatialIndex('%s', '%s');" % (colname(table), col)
                )
                self.create_cursor()
                self.execute(
                    "UPDATE %s SET %s = GeomFromText(%s, 4326);"
                    % (colname(table), col, col[: -len("-geom")])
                )
                self.commit()

    def create(self, path=None):
        if not path:
            path = self.datapackage + ".sqlite3"

        if os.path.exists(path):
            os.remove(path)

        self.connect(path)

        for table in self.tables:
            path = "%s/%s.csv" % (self.tables[table], table)
            fields = self.specification.schema[table]["fields"]
            key_field = self.specification.schema[table]["key-field"] or table
            field_datatype = {
                field: self.specification.field[field]["datatype"] for field in fields
            }

            # make a many-to-many table for each list
            joins = {}
            for field in fields:
                if self.specification.field[field]["cardinality"] == "n" and "%s|%s" % (
                    table,
                    field,
                ) not in [
                    "concat|fields",
                    "convert|parameters",
                    "endpoint|parameters",
                ]:
                    parent_field = self.specification.field[field]["parent-field"]
                    joins[field] = parent_field
                    field_datatype[parent_field] = self.specification.field[
                        parent_field
                    ]["datatype"]
                    fields.remove(field)

            self.create_cursor()
            self.create_table(table, fields, key_field, field_datatype)
            self.commit()

            self.create_cursor()
            self.load_csv(path, table, fields)
            self.commit()

            for split_field, field in joins.items():
                join_table = "%s_%s" % (table, field)

                self.create_cursor()
                self.create_table(
                    join_table,
                    [table, field],
                    None,
                    field_datatype,
                    unique=[table, field],
                )
                self.commit()

                self.create_cursor()
                self.load_join(
                    path,
                    join_table,
                    [table, field],
                    split_field=split_field,
                    field=field,
                )
                self.commit()

        for table, columns in self.indexes.items():
            self.index(table, columns)

        self.insert_metadata()

        self.disconnect()

    def insert_metadata(self):
        self.create_cursor()
        self.create_table(
            'metadata',
            ['id', 'end-date'],
            'id',
            {'id': 'integer', 'end-date': 'TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP'}
        )
        self.insert('metadata', ('end-date',), {'end-date': self._get_current_datetime()})
        self.commit()

    @staticmethod
    def _get_current_datetime():
        return datetime.now().isoformat()
