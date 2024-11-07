import os
# import json
import logging
from pathlib import Path
import sqlite3

# import numpy as np
# import pandas as pd
# import shapely.wkt
import duckdb

from .parquet import ParquetPackage

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


def get_schema(input_paths):
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


class DatasetParquetPackage(ParquetPackage):
    def __init__(self, dataset, organisation, **kwargs):
        super().__init__(dataset, tables=tables, indexes=indexes, **kwargs)
        self.dataset = dataset
        self.suffix = ".parquet"
        # self.entity_fields = self.specification.schema["entity"]["fields"]
        # self.organisations = organisation.organisation

    def load_facts(self, input_paths, output_path):
        logging.info(f"loading facts from {os.path.dirname(input_paths[0])}")

        fact_fields = self.specification.schema["fact"]["fields"]
        fields_str = ", ".join([f'"{field}"' if '-' in field else field for field in fact_fields])
        input_paths_str = ', '.join([f"'{path}'" for path in input_paths])

        schema_dict = get_schema(input_paths)

        con = duckdb.connect()
        print("\n\n")
        print(fields_str)
        print("\n\n")
        # Write a SQL query to load all csv files from the directory, group by a field, and get the latest record
        query = f"""
            SELECT {fields_str}
            FROM read_csv_auto(
                [{input_paths_str}],
                columns = {schema_dict}
            )
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY fact ORDER BY priority, "entry-date" DESC, "entry-number" DESC, resource
            ) = 1
        """

        con.execute(f"""
            COPY (
                {query}
            ) TO '{output_path}/fact{self.suffix}' (FORMAT PARQUET);
        """)

    def load_fact_resource(self, input_paths, output_path):
        logging.info(f"loading fact resources from {os.path.dirname(input_paths[0])}")

        fact_resource_fields = self.specification.schema["fact-resource"]["fields"]
        fields_str = ", ".join([f'"{field}"' if '-' in field else field for field in fact_resource_fields])
        input_paths_str = ', '.join([f"'{path}'" for path in input_paths])

        schema_dict = get_schema(input_paths)

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
            ) TO '{output_path}/fact_resource{self.suffix}' (FORMAT PARQUET);
        """)

    def load_entities(self, input_paths, output_path):
        logging.info(f"loading entities from {os.path.dirname(input_paths[0])}")

        entity_fields = self.specification.schema["entity"]["fields"]
        # Do this to match with later field names.
        entity_fields = [e.replace("-", "_") for e in entity_fields]
        input_paths_str = f"{output_path}/fact{self.suffix}"
        # input_paths_str = ', '.join([f"'{path}'" for path in input_paths])

        con = duckdb.connect()
        query = f"""
            SELECT DISTINCT REPLACE(field,'-','_')
            FROM parquet_scan('{str(input_paths_str)}')
        """

        # distinct_fields - list of fields in the field in fact
        rows = con.execute(query).fetchall()
        distinct_fields = [row[0] for row in rows]

        # json fields - list of fields which are present in the fact table which
        # do not exist separately in the entity table
        json_fields = [field for field in distinct_fields if field not in entity_fields]

        # null fields - list of fields which are not present in the fact tables which have
        # to be in the entity table as a column
        extra_fields = ['entity', 'dataset', 'typology', 'json', 'organisation_entity', 'organisation']
        null_fields = [field for field in entity_fields if field not in (distinct_fields + extra_fields)]

        # select fields - a list  of fields which have to be selected directly from the pivoted table
        # these are entity fields that are not null fields or a few special ones
        extra_fields = ['json', 'organisation_entity', 'dataset', 'typology', 'organisation']
        select_fields = [field for field in entity_fields if field not in null_fields + extra_fields]

        # set fields
        fields_to_include = ['entity', 'field', 'value']
        fields_str = ', '.join(fields_to_include)

        # Write a SQL query to load all parquet files from the directory, group by a field, and get the latest record
        query = f"""
            SELECT {fields_str}
            FROM (
                SELECT entity, {fields_str}, "entry-date"
                FROM parquet_scan('{str(input_paths_str)}')
                QUALIFY ROW_NUMBER() OVER (PARTITION BY fact,field,value ORDER BY priority, "entry-date" DESC) = 1
            )
            QUALIFY ROW_NUMBER() OVER (PARTITION BY entity,field ORDER BY "entry-date" DESC) = 1
        """

        # query = f"""
        #     SELECT {fields_str}
        #     FROM parquet_scan('{str(input_paths_str)}')
        #     QUALIFY ROW_NUMBER() OVER (PARTITION BY fact,field,value ORDER BY priority, "entry-date" DESC) = 1
        # """

        pivot_query = f"""
            PIVOT (
                {query}
            ) ON REPLACE(field,'-','_')
            USING MAX(value)
        """

        # now use the field lists produced above to create specific statements to:
        # add null columns which are missing
        # include columns in the json statement

        # Collate list of fields which don't exist but need to be in the final table
        select_statement = ', '.join([f"t1.{field}" for field in select_fields])
        null_fields_statement = ', '.join([f"NULL::VARCHAR AS \"{field}\"" for field in null_fields])
        json_statement = ', '.join([
            f"CASE WHEN t1.{field} IS NOT NULL THEN '{field}' ELSE NULL END, t1.{field}"
            for field in json_fields
        ])

        # define organisation query
        org_csv = './var/cache/organisation.csv'
        org_query = f"""
             SELECT * FROM read_csv_auto('{org_csv}')
         """

        dataset = Path(output_path).name

        sql = f"""
            INSTALL spatial; LOAD spatial;
            COPY(
                WITH computed_centroid AS (
                    SELECT 
                        * EXCLUDE (point),
                        CASE 
                            WHEN geometry IS NOT NULL AND point IS NULL 
                            THEN ST_AsText(ST_Centroid(ST_GeomFromText(geometry)))
                            ELSE point
                        END AS point
                    FROM (
                        SELECT '{dataset}' as dataset,
                        '{dataset}' as typology,
                        t2.entity as organisation_entity,
                        {select_statement},
                        {null_fields_statement},
                        json_object({json_statement}) as json,
                        FROM ({pivot_query}) as t1
                        LEFT JOIN ({org_query}) as t2
                        on t1.organisation = t2.organisation
                        )
                    ) 
                
                SELECT * FROM computed_centroid
            ) TO '{output_path}/entity{self.suffix}' (FORMAT PARQUET);
         """
        print(sql)
        con.execute(sql)

    def pq_to_sqlite(self, output_path):
        con = duckdb.connect()
        query = "LOAD sqlite;"
        con.execute(query)

        parquet_files = [fn for fn in os.listdir(output_path) if fn.endswith(self.suffix)]
        for parquet_file in parquet_files:
            sqlite_file = parquet_file.replace(self.suffix, ".sqlite3")
            con.execute("DROP TABLE IF EXISTS temp_table;")
            con.execute(f"""
                CREATE TABLE temp_table AS 
                SELECT * FROM parquet_scan('{output_path}/{parquet_file}');
            """)
            geom_columns_query = """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'temp_table'
                  AND (column_name ILIKE '%geom%' OR lower(column_name) = 'geometry' OR lower(column_name) = 'point');
            """
            geom_columns = [row[0] for row in con.execute(geom_columns_query).fetchall()]

            # Export the DuckDB table to the SQLite database
            con.execute(f"ATTACH DATABASE '{output_path}/{sqlite_file}' AS sqlite_db;")
            con.execute("DROP TABLE IF EXISTS sqlite_db.my_table;")
            con.execute("CREATE TABLE sqlite_db.my_table AS SELECT * FROM temp_table;")
            con.execute("DETACH DATABASE sqlite_db;")

            sqlite_con = sqlite3.connect(sqlite_file)
            sqlite_con.enable_load_extension(True)
            sqlite_con.execute('SELECT load_extension("mod_spatialite");')
            sqlite_con.execute("SELECT InitSpatialMetadata(1);")

            for geom in geom_columns:
                # Add geometry column with default SRID 4326 and geometry type
                if 'geometry' in geom:
                    sqlite_con.execute(f"SELECT AddGeometryColumn('my_table', '{geom}', 4326, 'MULTIPOLYGON', 2);")
                elif 'point' in geom:
                    sqlite_con.execute(f"SELECT AddGeometryColumn('my_table', '{geom}', 4326, 'POINT', 2);")
                # Create a spatial index on the geometry column
                sqlite_con.execute(f"SELECT CreateSpatialIndex('my_table', '{geom}');")
            sqlite_con.close()

        con.close()

    def load(self):
        pass
