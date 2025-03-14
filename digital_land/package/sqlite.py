import os
import sys
import csv
import sqlite3
import logging
import boto3
import botocore.exceptions
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


class SqlitePackage(Package):
    def __init__(self, *args, **kwargs):
        self.suffix = ".sqlite3"
        self._spatialite = None
        self.join_tables = {}
        self.fields = {}
        super().__init__(*args, **kwargs)

    def field_coltype(self, field):
        return coltype(self.specification.field[field]["datatype"])

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

    def connect(self):
        logging.debug(f"sqlite3 connect {self.path}")
        self.connection = sqlite3.connect(self.path)

        if self._spatialite:
            self.connection.enable_load_extension(True)
            self.connection.load_extension(self._spatialite)

    def disconnect(self):
        logging.debug("sqlite3 disconnect")
        self.connection.close()

    def create_table(self, table, fields, key_field=None, unique=None):
        self.execute(
            "CREATE TABLE %s (%s%s%s)"
            % (
                colname(table),
                ",\n".join(
                    [
                        "%s %s%s"
                        % (
                            colname(field),
                            self.field_coltype(field),
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
        value = str(row.get(field, ""))
        t = self.field_coltype(field)
        if t == "INTEGER":
            if value == "":
                return "NULL"
            return "%d" % Decimal(value)
        if t == "JSON":
            if value == "{}":
                return "NULL"
        return "'%s'" % value.replace("'", "''")

    def insert(self, table, fields, row, upsert=False):
        fields = [field for field in fields if not field.endswith("-geom")]
        self.execute(
            """
            INSERT OR REPLACE INTO %s(%s)
            VALUES (%s)%s;
            """
            % (
                colname(table),
                ",".join([colname(field) for field in fields]),
                ",".join(["%s" % self.colvalue(row, field) for field in fields]),
                " ON CONFLICT DO NOTHING " if upsert else "",
            )
        )

    def load_table(self, table, fields, path=None):
        logging.info("loading %s from %s" % (table, path))
        for row in csv.DictReader(open(path, newline="")):
            for field in row:
                if row.get(field, None) is None:
                    row[field] = ""
            self.insert(table, fields, row)

    def load_join_table(self, table, fields, split_field=None, field=None, path=None):
        logging.info("loading %s from %s" % (table, path))
        for row in csv.DictReader(open(path, newline="")):
            for value in row[split_field].split(";"):
                if value:
                    row[field] = value
                    self.insert(table, fields, row)

    def load(self, tables=None):
        tables = tables or self.tables
        fields, join_tables = self.get_table_fields(tables)
        for table in tables:
            table_fields = fields[table]
            path = "%s/%s.csv" % (tables[table], table)
            self.create_cursor()
            self.load_table(table, table_fields, path=path)
            self.commit()

        for join_table, join in join_tables.items():
            table = join["table"]
            field = join["field"]
            table_fields = [table, field]
            path = "%s/%s.csv" % (tables[table], table)
            self.create_cursor()
            self.load_join_table(
                join_table,
                fields=table_fields,
                split_field=join["split-field"],
                field=field,
                path=path,
            )
            self.commit()

    def get_table_fields(self, tables=None):
        """
        gets tables fields and join table information for a dictionary of tables
        """
        tables = tables or self.tables
        fields = {}
        join_tables = {}
        for table in tables:
            table_fields = self.specification.schema[table]["fields"]

            # a join table for each list field
            ignore = set()
            for field in table_fields:
                if self.specification.field[field]["cardinality"] == "n" and "%s|%s" % (
                    table,
                    field,
                ) not in [
                    "concat|fields",
                    "convert|parameters",
                    "endpoint|parameters",
                    "expect|datasets",
                    "expect|organisations",
                    "expect|parameters",
                    "expectation|parameters",
                ]:
                    parent_field = self.specification.field[field]["parent-field"]
                    join_table = "%s_%s" % (table, parent_field)
                    join_tables[join_table] = {
                        "table": table,
                        "field": parent_field,
                        "split-field": field,
                    }
                    ignore.add(field)

            table_fields = [field for field in table_fields if field not in ignore]
            fields[table] = table_fields
        return fields, join_tables

    def create_tables(self):
        self.fields, self.join_tables = self.get_table_fields(self.tables)
        for table in self.tables:
            key_field = table
            fields = self.fields[table]
            self.create_cursor()
            self.create_table(table, fields, key_field)
            self.commit()
        for join_table, join in self.join_tables.items():
            fields = [join["table"], join["field"]]
            self.create_cursor()
            self.create_table(join_table, fields, unique=fields)
            self.commit()

    def create_index(self, table, fields, name=None):
        if type(fields) is not list:
            fields = [fields]
        cols = [colname(field) for field in fields if not field.endswith("-geom")]
        if not name:
            name = colname(table) + "_on_" + "__".join(cols) + "_index"
        if cols:
            logging.info("creating index %s" % (name))
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

    def create_indexes(self, tables=None):
        tables = tables or self.tables
        table_fields, join_tables = self.get_table_fields(tables)
        for table, index_fields in self.indexes.items():
            # check to see index is in the tables or in the join tables
            if table in tables.keys() or table in join_tables:
                for fields in index_fields:
                    self.create_index(table, fields)

    def drop_index(self, table, fields, name=None):
        if type(fields) is not list:
            fields = [fields]
        cols = [colname(field) for field in fields if not field.endswith("-geom")]
        if not name:
            name = colname(table) + "_on_" + "__".join(cols) + "_index"
        if cols:
            logging.info("dropping index %s" % (name))
            self.execute("DROP INDEX IF EXISTS %s;" % (name))
        # TODO add support to drop spatialite indexes if they're needed

    def drop_indexes(self):
        for table, index_fields in self.indexes.items():
            for fields in index_fields:
                self.drop_index(table, fields)

    def create_database(self):
        if os.path.exists(self.path):
            os.remove(self.path)

        self.set_up_connection()

        self.create_tables()

    def set_up_connection(self):
        self.connect()

        if self._spatialite:
            self.connection.execute("select InitSpatialMetadata(1)")

    def create(self):
        self.create_database()
        self.load()
        self.create_indexes()
        self.disconnect()

    def load_from_s3(self, bucket_name, object_key, table_name):
        # Ensure parameters are valid
        if not isinstance(bucket_name, str) or not isinstance(object_key, str):
            raise ValueError("Bucket name and object key must be strings.")

        local_path = os.path.dirname(self.path)
        s3 = boto3.client("s3")

        file_key = f"{table_name}.sqlite3"
        local_file_path = os.path.join(local_path, file_key)

        try:
            os.makedirs(local_path, exist_ok=True)  # Ensure local directory exists
            s3.download_file(bucket_name, object_key + "/" + file_key, local_file_path)
        except botocore.exceptions.NoCredentialsError:
            logger.error(
                "❌ AWS credentials not found. Run `aws configure` to set them."
            )
        except botocore.exceptions.ParamValidationError as e:
            logger.error(f"❌ Parameter validation error: {e}")
        except botocore.exceptions.ClientError as e:
            logger.error(f"❌ AWS S3 error: {e}")

        self.set_up_connection()
        self.load()
        # self.create_indexes()# Do we need this?
        self.disconnect()
