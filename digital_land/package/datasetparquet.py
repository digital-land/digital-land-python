import os
import json
import logging
from decimal import Decimal
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
        self.entity_fields = self.specification.schema["entity"]["fields"]
        self.organisations = organisation.organisation

    def migrate_entity(self, row):
        dataset = self.dataset
        entity = row.get("entity", "")

        if not entity:
            logging.error(f"{dataset} entity with a missing entity number")
            return None

        row["dataset"] = dataset
        row["typology"] = self.specification.schema[dataset]["typology"]
        row["name"] = row.get("name", "")

        if not row.get("reference", ""):
            logging.error(f"entity {entity}: missing reference")

        if not row.get("organisation-entity", ""):
            row["organisation-entity"] = self.organisations.get(
                row.get("organisation", ""), {}
            ).get("entity", "")

        properties = {
            field: row[field]
            for field in row
            if row[field] and field not in [
                "geography",
                "geometry",
                "organisation",
                "reference",
                "prefix",
                "point",
                "slug"
            ] + self.entity_fields
        }
        row["json"] = json.dumps(properties) if properties else None

        shape = row.get("geometry", "")
        point = row.get("point", "")
        wkt = shape or point

        if wkt:
            if not row.get("latitude", ""):
                try:
                    geometry = shapely.wkt.loads(wkt)
                    geometry = geometry.centroid
                    row["longitude"] = "%.6f" % round(Decimal(geometry.x), 6)
                    row["latitude"] = "%.6f" % round(Decimal(geometry.y), 6)
                except Exception as e:
                    logging.error(f"error processing wkt {wkt} for entity {entity}")
                    logging.error(e)
                    return None

            if not row.get("point", ""):
                row["point"] = "POINT(%s %s)" % (row["longitude"], row["latitude"])

        return row

    def entity_row(self, facts):
        row = {"entity": facts[0][0]}
        for fact in facts:
            row[fact[1]] = fact[2]
        return row

    def load_old_entities(self, path, chunksize=chunk_size):
        # fields = self.specification.schema["old-entity"]["fields"]
        entity_min = int(self.specification.schema[self.dataset].get("entity-minimum"))
        entity_max = int(self.specification.schema[self.dataset].get("entity-maximum"))

        logging.info(f"loading old-entity from {path}")
        rows_to_insert = []
        for chunk in pd.read_csv(path, chunksize=chunksize):
            for _, row in chunk.iterrows():
                entity_id = int(row.get("old-entity"))
                if entity_min <= entity_id <= entity_max:
                    rows_to_insert.append(row)
                if len(rows_to_insert) >= chunk_size:
                    self.append_to_parquet("old-entity", rows_to_insert)
                    rows_to_insert.clear()  # Clear the list for the next chunk
                    gc.collect()

        if rows_to_insert:
            self.append_to_parquet("old-entity", rows_to_insert)

    def load_entities(self):
        conn = duckdb.connect()
        query = f"""
            SELECT entity, field, value FROM f'{self.path}fact{self.suffix}'
            WHERE value != '' OR field == 'end-date'
            ORDER BY entity, field, priority desc, entry_date
        """
        results = conn.execute(query).fetchall()

        facts = []
        rows_to_insert = []

        for fact in results:
            if facts and fact[0] != facts[0][0]:
                row = self.entity_row(facts)
                row = self.migrate_entity(row)
                if row:
                    rows_to_insert.append(row)

                # If we reach the chunk size, write to parquet
                if len(rows_to_insert) >= chunk_size:
                    self.append_to_parquet("entity", rows_to_insert)
                    rows_to_insert.clear()  # Clear the list for the next chunk
                    gc.collect()

                    facts = []

                facts.append(fact)

        # Handle the last group of current_facts
        if facts:
            row = self.entity_row(facts)
            row = self.migrate_entity(row)
            if row:
                rows_to_insert.append(row)

        # Write any remaining rows to parquet
        if rows_to_insert:
            self.append_to_parquet("entity", rows_to_insert)

    def add_counts(self):
        """count the number of entities by resource"""
        query = f"""
            SELECT resource, COUNT(*)
            FROM (
                SELECT DISTINCT resource, f.entity
                FROM 
                    read_parquet(f'{self.path}/resource{self.suffix}') AS resource
                JOIN 
                    read_parquet(f'{self.path}/fact{self.suffix}') AS f
                JOIN 
                    read_parquet(f'{self.path}/fact-resource{self.suffix}') AS fr
                ON 
                    f.entity = fr.entity AND f.fact = fr.fact
            ) AS subquery
            GROUP BY resource
        """
        conn = duckdb.connect()
        dataset_resource = conn.execute(query).fetchall()

        self.append_to_parquet("dataset_resource", dataset_resource)

    def entry_date_upsert(self, table, fields, data, conflict_fields, update_fields):
        """
        Dataset specific upsert function that only replace values for more recent entry_dates.
        Will insert rows where no conflict is found. where there's a conflict it was compare entry dates
        and insert other field
        """
        # print("\nIn entry_date_upsert")
        # print("table")
        # print(table)
        parquet_path = self.get_parquet_path(table)
        # print("\nparquet_path")
        # print(parquet_path)
        if len(data) == 391:
            print(parquet_path)
            print(data.shape)
        # try:
        #     conn = duckdb.connect()
        #
        #     # Convert the new data to a temporary DuckDB table for batch processing
        #     conn.register("temp_data", data)
        #
        #     # query for upserting data
        #     conn.execute(
        #         """
        #         INSERT INTO %s(%s)
        #         SELECT %s FROM temp_data
        #         ON CONFLICT(%s) DO UPDATE SET %s
        #         WHERE excluded.entry_date>%s.entry_date
        #         ;
        #         """
        #         % (
        #             parquet_path,
        #             ",".join([colname(field) for field in fields]),
        #             ",".join([colname(field) for field in fields]),
        #             ",".join([colname(field) for field in conflict_fields]),
        #             ", ".join(
        #                 [
        #                     "%s=excluded.%s" % (colname(field), colname(field))
        #                     for field in update_fields
        #                 ]
        #             ),
        #             parquet_path,
        #         )
        #     )
        #
        #     conn.close()
        # except Exception as e:
        #     logging.error(f"Failed to upsert data to '{parquet_path}': {e}")

    def load_facts(self, path, chunksize=chunk_size):
        logging.info(f"loading facts from {path}")

        # print("In load_facts")
        # print(path)

        fact_fields = self.specification.schema["fact"]["fields"]
        # fact_resource_fields = self.specification.schema["fact-resource"]["fields"]
        fact_conflict_fields = ["fact"]
        fact_update_fields = [
            field for field in fact_fields if field not in fact_conflict_fields
        ]

        # print("pre chunking\n")
        for chunk in pd.read_csv(path, chunksize=chunksize):
            self.entry_date_upsert(
                "fact", fact_fields, chunk, fact_conflict_fields, fact_update_fields
            )

            # # Append the entire chunk at once for fact-resource
            # self.append_to_parquet("fact-resource", chunk)

    def load_column_fields(self, path, chunksize=chunk_size):
        # fields = self.specification.schema["column-field"]["fields"]
        logging.info(f"loading column_fields from {path}")

        for chunk in pd.read_csv(path, chunksize=chunksize):
            rows_to_insert = []
            for _, row in chunk.iterrows():
                row["resource"] = path.stem
                row["dataset"] = self.dataset
                rows_to_insert.append(row)
            self.append_to_parquet("column-field", rows_to_insert)
            rows_to_insert.clear()

    def load_issues(self, path, chunksize=chunk_size):
        # fields = self.specification.schema["issue"]["fields"]
        logging.info(f"loading issues from {path}")

        for chunk in pd.read_csv(path, chunksize=chunksize):
            rows_to_insert = []
            for _, row in chunk.iterrows():
                rows_to_insert.append(row)
            self.append_to_parquet("issue", rows_to_insert)
            rows_to_insert.clear()

    def load_dataset_resource(self, path, chunksize=chunk_size):
        """
        Load dataset-resource from a CSV file in chunks.

        param path: The path to the CSV file.
        param chunksize: The number of rows to read per chunk.
        """
        # fields = self.specification.schema["dataset-resource"]["fields"]

        logging.info(f"loading dataset-resource from {path}")

        chunk_iterator = pd.read_csv(path, chunksize=chunksize)

        for chunk in chunk_iterator:
            # Process the chunk (e.g., modify fields or add any necessary data)
            self.append_to_parquet("dataset-resource", chunk)

        logging.info("Finished loading dataset-resource")

    def get_parquet_path(self, table_name):
        ##########################################################################
        # self.path references the full name of the sqlite3 file
        # Remove and find a way of adding this to the cli arguments or otherwise #
        ##########################################################################
        return os.path.join(self.path.removesuffix(".sqlite3"), f"{table_name}{self.suffix}")

    def append_to_parquet(self, table_name, data):
        """
        Append data to a Parquet file. If the file doesn't exist, it will create a new one.

        param table_name: Name of the Parquet file (without extension).
        param data: Pandas DataFrame or dictionary containing data to append.
        """
        parquet_path = self.get_parquet_path(table_name)
        print("In append_to_parquet")
        print("parquet_path")
        print(parquet_path)
        if isinstance(data, dict):
            data = pd.DataFrame([data])
        if not os.path.exists(parquet_path):
            # Write new Parquet file
            data.to_parquet(parquet_path, index=False)
        else:
            try:
                # Append data to current parquet file. If any issues read in current values and concatenate
                # Connect to DuckDB (in-memory or specify a file if needed)
                conn = duckdb.connect()

                # Create a DuckDB SQL query to append new data to the Parquet file
                conn.execute(f"""
                    INSERT INTO '{parquet_path}'
                    SELECT * FROM new_data_df
                """)

                # Close the connection
                conn.close()
            except duckdb.IOException as e:
                logging.error(f"DuckDB error, reading in then appending parquet files: {e}")
                try:
                    # Append to existing Parquet file
                    existing_data = pd.read_parquet(parquet_path)
                    combined_data = pd.concat([existing_data, data], ignore_index=True)
                    combined_data.to_parquet(parquet_path, index=False)
                except (pd.errors.EmptyDataError, FileNotFoundError) as e:
                    logging.error(f"Failed to read existing data or append new data to '{parquet_path}': {e}")
                except Exception as e:
                    logging.error(f"Failed to append data to '{parquet_path}': {e}")

    def read_from_parquet(self, table_name, columns=None):
        """
        Read data from a Parquet file and return a DataFrame.

        param table_name: Name of the Parquet file (without extension)
        param columns: Optional list of columns to read
        return: DataFrame containing the data
        """
        parquet_path = self.get_parquet_path(table_name)
        if not os.path.exists(parquet_path):
            raise FileNotFoundError(f"Parquet file '{parquet_path}' not found.")

        return pd.read_parquet(parquet_path, columns=columns)

    def update_parquet(self, table_name, key_field, new_data):
        """
        Update specific rows in a Parquet file. This replaces rows based on a unique key.

        param table_name: Name of the Parquet file (without extension)
        param key_field: The field that serves as the unique key for updates
        param new_data: A dictionary or DataFrame containing new data
        """
        parquet_path = self.get_parquet_path(table_name)
        if isinstance(new_data, dict):
            new_data = pd.DataFrame([new_data])

        if not os.path.exists(parquet_path):
            raise FileNotFoundError(f"Parquet file '{parquet_path}' not found.")

        existing_data = pd.read_parquet(parquet_path)
        # Update rows by merging on the key_field
        updated_data = pd.concat(
            [existing_data[~existing_data[key_field].isin(new_data[key_field])], new_data],
            ignore_index=True
        )
        # Write back the updated data
        updated_data.to_parquet(parquet_path, index=False)

    def load(self):
        pass