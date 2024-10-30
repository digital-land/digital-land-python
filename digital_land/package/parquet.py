import gc
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import logging
from .package import Package
import duckdb

logger = logging.getLogger(__name__)


def colname(field):
    if field == "default":
        return "_default"
    return field.replace("-", "_")


def coltype(datatype):
    if datatype == "integer":
        return pa.int64()
    elif datatype == "float":
        return pa.float64()
    elif datatype == "json":
        return pa.string()  # JSON can be stored as string
    else:
        return pa.string()


class ParquetPackage(Package):
    def __init__(self, *args, **kwargs):
        self.suffix = ".parquet"
        super().__init__(*args, **kwargs)

    def field_coltype(self, field):
        return coltype(self.specification.field[field]["datatype"])

    def load_table(self, table, fields, path=None, chunksize=10000):
        """
        Load data from a CSV file in batches and write to Parquet incrementally.
        """
        logging.info(f"Loading {table} from {path} in batches of {chunksize}")
        parquet_path = f"{self.path}/{table}{self.suffix}"

        # # Identify geometry fields
        # geom_columns = [colname(field) for field in fields if field.endswith("-geom")]
        # non_geom_columns = [colname(field) for field in fields if not field.endswith("-geom")]

        first_batch = True
        for chunk in pd.read_csv(path, chunksize=chunksize):
            #, usecols=non_geom_columns + geom_columns):
            df = pd.DataFrame(chunk)

            # Convert to Parquet-compatible table
            table_data = pa.Table.from_pandas(df)

            # Write or append to Parquet file
            if first_batch:
                pq.write_table(table_data, parquet_path, row_group_size=chunksize)
                first_batch = False
            else:
                with pq.ParquetWriter(parquet_path, table_data.schema, use_dictionary=True) as writer:
                    writer.write_table(table_data)

            # Do a garbage collect to free up memory immediately
            del df, table_data
            gc.collect()

        logging.info(f"Completed loading and saving {table} to {parquet_path}")

    def load_join_table(self, table, fields, split_field=None, field=None, path=None, chunk_size=100000):
        logging.info("loading %s from %s" % (table, path))

        # Prepare the output path for the Parquet file
        parquet_path = f"{self.path}/{table}{self.suffix}"
        print("\n\n")
        print("parquet_path:")
        print(parquet_path)
        print("\n\n")

        # Initialize a DataFrame to store the data temporarily
        temp_data = []

        # Read the CSV file in chunks
        for chunk in pd.read_csv(path, chunksize=chunk_size):
            # Loop over every row
            for _, row in chunk.iterrows():
                # Split the values in the split field and store each value if they exist
                for value in row[split_field].split(";"):
                    if value:
                        new_row = row.copy()
                        new_row[field] = value
                        temp_data.append(new_row[fields].to_dict())

            # Convert the temporary data to a DataFrame and append to Parquet file
            if temp_data:
                df = pd.DataFrame(temp_data)
                df.to_parquet(parquet_path, index=False, engine='pyarrow', append=True)  # Append to the Parquet file
                # Clear not needed data and garbage collect
                temp_data = []
                del df
                gc.collect()

        logging.info("Finished loading %s into Parquet format" % table)

    def get_table_fields(self, tables=None):
        """
        Gets tables fields and join table information for a dictionary of tables.
        Adapts for Parquet by defining relationships and handling list fields as necessary.
        """
        tables = tables or self.tables
        fields = {}
        join_tables = {}

        for table in tables:
            # Extract fields for the given table from the specification
            table_fields = self.specification.schema[table]["fields"]

            # Handle fields that need to be represented as join tables (many-to-many relationships)
            ignore = set()
            for field in table_fields:
                if self.specification.field[field]["cardinality"] == "n" and f"{table}|{field}" not in [
                    "concat|fields",
                    "convert|parameters",
                    "endpoint|parameters",
                ]:
                    # Identify the parent field to create a join table entry
                    parent_field = self.specification.field[field]["parent-field"]
                    join_table = f"{table}_{parent_field}"

                    # Add the join table entry
                    join_tables[join_table] = {
                        "table": table,
                        "field": parent_field,
                        "split-field": field,
                    }

                    # Exclude this field from the main table
                    ignore.add(field)

            # Include only fields that are not managed as join tables
            table_fields = [field for field in table_fields if field not in ignore]
            fields[table] = table_fields

        return fields, join_tables

    def load(self, tables=None):
        """
        Load data from csv into parquet files
        """
        print("\n\n")
        print("self.path:")
        print(self.path)
        print("\n\n")
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        tables = tables or self.tables
        fields, join_tables = self.get_table_fields(tables)
        print("\n\n")
        print("Tables:")
        print(tables)
        print("\n\n")
        for table in tables:
            table_fields = fields[table]
            path = "%s/%s.csv" % (tables[table], table)
            self.load_table(table, table_fields, path=path)

        for join_table, join in join_tables.items():
            table = join["table"]
            field = join["field"]
            table_fields = [table, field]
            path = "%s/%s.csv" % (tables[table], table)
            self.load_join_table(
                join_table,
                fields=table_fields,
                split_field=join["split-field"],
                field=field,
                path=path,
            )

    def parquet_to_sqlite(self, tables=None):
        """
        Copy parquet files to sqlite files
        """
        # Create a DuckDB connection and ensure SQLite extension is installed
        con = duckdb.connect()
        con.execute("INSTALL sqlite; LOAD sqlite;")

        tables = tables or self.tables
        for table in tables:
            # Get parquet files
            parquet_path = f"{self.path}/{table}{self.suffix}"
            sqlite_path = f"{self.path}/{table}.sqlite3"

            # Create the table from Parquet in DuckDB's in-memory schema
            try:
                query = f"CREATE TABLE parquet_table AS SELECT * FROM parquet_scan('{str(parquet_path)}')"
                con.execute(query)
            except Exception as e:
                logging.error(f"Error creating table 'parquet_table': {e}")

            # Check if the table exists
            try:
                con.execute("SELECT * FROM parquet_table LIMIT 1;")
            except Exception as e:
                logging.error(f"Table 'parquet_table' does not exist: {e}")

            # Attach the SQLite database
            try:
                query = f"ATTACH '{sqlite_path}' AS sqlite_db (TYPE SQLITE);"
                con.execute(query)
            except Exception as e:
                logging.error(f"Error attaching SQLite database: {e}")

            # Drop the table from sqlite if it exists, as otherwise you insert it on top of current table
            try:
                query = "DROP TABLE IF EXISTS sqlite_db.parquet_table;"
                con.execute(query)
            except Exception as e:
                logging.error(f"Error creating target table in SQLite: {e}")

            # Create the target table in SQLite based on the DuckDB table structure
            # Need this to create initial table / file
            try:
                query = "CREATE TABLE IF NOT EXISTS sqlite_db.parquet_table AS SELECT * FROM parquet_table WHERE 0;"
                con.execute(query)
            except Exception as e:
                logging.error(f"Error creating target table in SQLite: {e}")

            # Insert data into SQLite database
            try:
                query = f"INSERT INTO sqlite_db.parquet_table SELECT * FROM parquet_table;"
                con.execute(query)
            except Exception as e:
                logging.error(f"Error inserting data into SQLite database: {e}")

            # Close the DuckDB connection
            con.close()

    def create(self):
        # self.create_parquet_files()
        self.load()
        # self.parquet_to_sqlite()
