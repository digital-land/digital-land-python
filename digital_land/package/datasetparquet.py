import os
import json
import logging
from decimal import Decimal

import numpy as np
import pandas as pd
import shapely.wkt
import duckdb
import gc

from .parquet import ParquetPackage, colname

logger = logging.getLogger(__name__)

chunk_size = 100000

# TBD: move to from specification datapackage definition
tables = {
    "dataset-resource": None,
    "column-field": None,
    "issue": None,
    "entity": None,
    "old-entity": None,
    "fact": None,
    "fact-resource": None,
}

# TBD: infer from specification dataset
indexes = {
    "old-entity": ["entity", "old-entity", "status"],
    "fact": ["entity"],
    "fact-resource": ["fact", "resource"],
    "column-field": ["dataset", "resource", "column", "field"],
    "issue": ["resource", "dataset", "field"],
    "dataset-resource": ["resource"],
}

class DatasetParquetPackage(ParquetPackage):
    def __init__(self, dataset, organisation, **kwargs):
        super().__init__(dataset, tables=tables, indexes=indexes, **kwargs)
        self.dataset = dataset
        self.suffix = ".parquet"
        # self.entity_fields = self.specification.schema["entity"]["fields"]
        # self.organisations = organisation.organisation

    def get_schema(self, input_paths):
        # There are issues with the schema when reading in lots of files, namely smaller files have few or zero rows
        # Plan is to find the largest file, create an initial database schema from that then use that in future
        largest_file = max(input_paths, key=os.path.getsize)

        con = duckdb.connect()

        # drop_temp_table_query = "DROP TABLE IF EXISTS temp_table;"
        # con.query(drop_temp_table_query)

        create_temp_table_query = f"""
            DROP TABLE IF EXISTS temp_table;
            CREATE TEMP TABLE temp_table AS
            SELECT * FROM read_csv_auto('{largest_file}')
            LIMIT 1000;
        """
        con.query(create_temp_table_query)

        # Extract the schema from the temporary table
        schema_query = """
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'temp_table';
        """
        schema_df = con.query(schema_query).df()

        return dict(zip(schema_df['column_name'], schema_df['data_type']))

    def load_facts(self, input_paths, output_path):
        logging.info(f"loading facts from {os.path.dirname(input_paths[0])}")

        fact_fields = self.specification.schema["fact"]["fields"]
        fields_str = ", ".join([f'"{field}"' if '-' in field else field for field in fact_fields])
        input_paths_str = ', '.join([f"'{path}'" for path in input_paths])

        schema_dict = self.get_schema(input_paths)

        con = duckdb.connect()
        # Write a SQL query to load all csv files from the directory, group by a field, and get the latest record
        query = f"""
            SELECT {fields_str}
            FROM read_csv_auto(
                [{input_paths_str}],
                columns = {schema_dict}
            )
            QUALIFY ROW_NUMBER() OVER (PARTITION BY fact ORDER BY priority, "entry-date" DESC) = 1
        """

        con.execute(f"""
            COPY (
                {query}
            ) TO '{output_path}/fact.parquet' (FORMAT PARQUET);
        """)

    def load_fact_resource(self, input_paths, output_path):
        logging.info(f"loading fact resources from {os.path.dirname(input_paths[0])}")

        fact_resource_fields = self.specification.schema["fact-resource"]["fields"]
        fields_str = ", ".join([f'"{field}"' if '-' in field else field for field in fact_resource_fields])
        input_paths_str = ', '.join([f"'{path}'" for path in input_paths])

        schema_dict = self.get_schema(input_paths)

        con = duckdb.connect()
        # Write a SQL query to load all csv files from the directory, group by a field, and get the latest record
        query = f"""
            SELECT {fields_str}
            FROM read_csv_auto(
                [{input_paths_str}],
                columns = {schema_dict}
            )
        """

        con.execute(f"""
            COPY (
                {query}
            ) TO '{output_path}/fact_resource.parquet' (FORMAT PARQUET);
        """)

    def load_entities(self, input_paths, output_path):
        logging.info(f"loading entities from {os.path.dirname(input_paths[0])}")

        entity_fields = self.specification.schema["entity"]["fields"]
        fields_str = ", ".join([f'"{field}"' if '-' in field else field for field in entity_fields])
        input_paths_str = ', '.join([f"'{path}'" for path in input_paths])

        schema_dict = self.get_schema(input_paths)

        con = duckdb.connect()
        # Write a SQL query to load all csv files from the directory, group by a field, and get the latest record
        query = f"""
            SELECT DISTINCT REPLACE(field,'-','_')
            FROM read_csv_auto(
                [{input_paths_str}],
                columns = {schema_dict}
            )
        """

        # distinct_fields - list of fields in the field field in fact
        rows = con.execute(query).fetchall()
        print(rows.head(4))
        print("\n")
        print(len(rows))
        print("\n")
        distinct_fields = [row[0] for row in rows]
        print(len(distinct_fields))
        print("\n")



    # def load_column_fields(self, path, chunksize=chunk_size):
    #     # fields = self.specification.schema["column-field"]["fields"]
    #     logging.info(f"loading column_fields from {path}")
    #
    #     for chunk in pd.read_csv(path, chunksize=chunksize):
    #         rows_to_insert = []
    #         for _, row in chunk.iterrows():
    #             row["resource"] = path.stem
    #             row["dataset"] = self.dataset
    #             rows_to_insert.append(row)
    #         self.append_to_parquet("column-field", rows_to_insert)
    #         rows_to_insert.clear()
    #
    # def load_issues(self, path, chunksize=chunk_size):
    #     # fields = self.specification.schema["issue"]["fields"]
    #     logging.info(f"loading issues from {path}")
    #
    #     for chunk in pd.read_csv(path, chunksize=chunksize):
    #         rows_to_insert = []
    #         for _, row in chunk.iterrows():
    #             rows_to_insert.append(row)
    #         self.append_to_parquet("issue", rows_to_insert)
    #         rows_to_insert.clear()

    # def load_dataset_resource(self, path, chunksize=chunk_size):
    #     """
    #     Load dataset-resource from a CSV file in chunks.
    #
    #     param path: The path to the CSV file.
    #     param chunksize: The number of rows to read per chunk.
    #     """
    #     # fields = self.specification.schema["dataset-resource"]["fields"]
    #
    #     logging.info(f"loading dataset-resource from {path}")
    #
    #     chunk_iterator = pd.read_csv(path, chunksize=chunksize)
    #
    #     for chunk in chunk_iterator:
    #         # Process the chunk (e.g., modify fields or add any necessary data)
    #         self.append_to_parquet("dataset-resource", chunk)
    #
    #     logging.info("Finished loading dataset-resource")

    # def get_parquet_path(self, table_name):
    #     ##########################################################################
    #     # self.path references the full name of the sqlite3 file
    #     # Remove and find a way of adding this to the cli arguments or otherwise #
    #     ##########################################################################
    #     return os.path.join(self.path.removesuffix(".sqlite3"), f"{table_name}{self.suffix}")
    #
    # def append_to_parquet(self, table_name, data):
    #     """
    #     Append data to a Parquet file. If the file doesn't exist, it will create a new one.
    #
    #     param table_name: Name of the Parquet file (without extension).
    #     param data: Pandas DataFrame or dictionary containing data to append.
    #     """
    #     parquet_path = self.get_parquet_path(table_name)
    #     print("In append_to_parquet")
    #     print("parquet_path")
    #     print(parquet_path)
    #     if isinstance(data, dict):
    #         data = pd.DataFrame([data])
    #     if not os.path.exists(parquet_path):
    #         # Write new Parquet file
    #         data.to_parquet(parquet_path, index=False)
    #     else:
    #         try:
    #             # Append data to current parquet file. If any issues read in current values and concatenate
    #             # Connect to DuckDB (in-memory or specify a file if needed)
    #             conn = duckdb.connect()
    #
    #             # Create a DuckDB SQL query to append new data to the Parquet file
    #             conn.execute(f"""
    #                 INSERT INTO '{parquet_path}'
    #                 SELECT * FROM new_data_df
    #             """)
    #
    #             # Close the connection
    #             conn.close()
    #         except duckdb.IOException as e:
    #             logging.error(f"DuckDB error, reading in then appending parquet files: {e}")
    #             try:
    #                 # Append to existing Parquet file
    #                 existing_data = pd.read_parquet(parquet_path)
    #                 combined_data = pd.concat([existing_data, data], ignore_index=True)
    #                 combined_data.to_parquet(parquet_path, index=False)
    #             except (pd.errors.EmptyDataError, FileNotFoundError) as e:
    #                 logging.error(f"Failed to read existing data or append new data to '{parquet_path}': {e}")
    #             except Exception as e:
    #                 logging.error(f"Failed to append data to '{parquet_path}': {e}")
    #
    # def read_from_parquet(self, table_name, columns=None):
    #     """
    #     Read data from a Parquet file and return a DataFrame.
    #
    #     param table_name: Name of the Parquet file (without extension)
    #     param columns: Optional list of columns to read
    #     return: DataFrame containing the data
    #     """
    #     parquet_path = self.get_parquet_path(table_name)
    #     if not os.path.exists(parquet_path):
    #         raise FileNotFoundError(f"Parquet file '{parquet_path}' not found.")
    #
    #     return pd.read_parquet(parquet_path, columns=columns)
    #
    # def update_parquet(self, table_name, key_field, new_data):
    #     """
    #     Update specific rows in a Parquet file. This replaces rows based on a unique key.
    #
    #     param table_name: Name of the Parquet file (without extension)
    #     param key_field: The field that serves as the unique key for updates
    #     param new_data: A dictionary or DataFrame containing new data
    #     """
    #     parquet_path = self.get_parquet_path(table_name)
    #     if isinstance(new_data, dict):
    #         new_data = pd.DataFrame([new_data])
    #
    #     if not os.path.exists(parquet_path):
    #         raise FileNotFoundError(f"Parquet file '{parquet_path}' not found.")
    #
    #     existing_data = pd.read_parquet(parquet_path)
    #     # Update rows by merging on the key_field
    #     updated_data = pd.concat(
    #         [existing_data[~existing_data[key_field].isin(new_data[key_field])], new_data],
    #         ignore_index=True
    #     )
    #     # Write back the updated data
    #     updated_data.to_parquet(parquet_path, index=False)

    def load(self):
        pass