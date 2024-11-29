import os
import logging
import duckdb
from .package import Package

logger = logging.getLogger(__name__)

# TBD: move to from specification datapackage definition
tables = {
    "dataset-resource": None,
    "column-field": None,
    "issue": None,
    "entity": None,
    "fact": None,
    "fact-resource": None,
}

# TBD: infer from specification dataset
indexes = {
    "fact": ["entity"],
    "fact-resource": ["fact", "resource"],
    "column-field": ["dataset", "resource", "column", "field"],
    "issue": ["resource", "dataset", "field"],
    "dataset-resource": ["resource"],
}


class DatasetParquetPackage(Package):
    def __init__(self, dataset, organisation, cache_dir, **kwargs):
        self.suffix = ".parquet"
        super().__init__(dataset, tables=tables, indexes=indexes, **kwargs)
        self.dataset = dataset
        self.organisation = organisation
        self.cache_dir = cache_dir
        self._spatialite = None
        # Persistent connection for the class. Given name to ensure that table is stored on disk (not purely in memory)
        os.makedirs(cache_dir, exist_ok=True)
        self.duckdb_file = os.path.join(cache_dir, f"{dataset}.duckdb")
        self.conn = duckdb.connect(self.duckdb_file)
        self.schema = self.get_schema()
        self.typology = self.specification.schema[dataset]["typology"]

    def get_schema(self):
        schema = {}

        for field in sorted(
            list(
                set(self.specification.schema["fact"]["fields"]).union(
                    set(self.specification.schema["fact-resource"]["fields"])
                )
            )
        ):
            datatype = self.specification.field[field]["datatype"]
            schema[field] = "BIGINT" if datatype == "integer" else "VARCHAR"

        return schema

    def create_temp_table(self, input_paths):
        # Create a temp table of the data from input_paths as we need the information stored there at various times
        logging.info(
            f"loading data into temp table from {os.path.dirname(input_paths[0])}"
        )

        input_paths_str = ", ".join([f"'{path}'" for path in input_paths])

        self.conn.execute("DROP TABLE IF EXISTS temp_table")
        query = f"""
            CREATE TEMPORARY TABLE temp_table AS
            SELECT *
            FROM read_csv(
                [{input_paths_str}],
                columns = {self.schema},
                header = true,
                force_not_null = {[field for field in self.schema.keys()]}
            )
        """
        self.conn.execute(query)

    def load_facts(self):
        logging.info("loading facts from temp table")

        fact_fields = self.specification.schema["fact"]["fields"]
        fields_str = ", ".join(
            [f'"{field}"' if "-" in field else field for field in fact_fields]
        )

        # query to extract data from the temp table (containing raw data), group by a fact, and get the highest
        # priority or latest record
        query = f"""
            SELECT {fields_str}
            FROM temp_table
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY fact ORDER BY priority, "entry-date" DESC, "entry-number" DESC
            ) = 1
        """

        self.conn.execute(
            f"""
            COPY (
                {query}
            ) TO '{self.cache_dir}/fact{self.suffix}' (FORMAT PARQUET);
        """
        )

    def load_fact_resource(self):
        logging.info("loading fact resources from temp table")

        fact_resource_fields = self.specification.schema["fact-resource"]["fields"]
        fields_str = ", ".join(
            [f'"{field}"' if "-" in field else field for field in fact_resource_fields]
        )

        # All CSV files have been loaded into a temporary table. Extract several columns and export
        query = f"""
            SELECT {fields_str}
            FROM temp_table
        """

        self.conn.execute(
            f"""
            COPY (
                {query}
            ) TO '{self.cache_dir}/fact_resource{self.suffix}' (FORMAT PARQUET);
        """
        )

    def load_entities(self):
        organisation_path = self.organisation.organisation_path

        logging.info("loading entities from temp table")

        entity_fields = self.specification.schema["entity"]["fields"]
        # Do this to match with later field names.
        entity_fields = [e.replace("-", "_") for e in entity_fields]
        input_paths_str = f"{self.cache_dir}/fact{self.suffix}"

        query = f"""
            SELECT DISTINCT REPLACE(field,'-','_')
            FROM parquet_scan('{str(input_paths_str)}')
        """

        # distinct_fields - list of fields in the field in fact
        rows = self.conn.execute(query).fetchall()
        distinct_fields = [row[0] for row in rows]

        # json fields - list of fields which are present in the fact table which
        # do not exist separately in the entity table
        # Need to ensure that 'organisation' is not included either
        json_fields = [
            field
            for field in distinct_fields
            if field not in entity_fields + ["organisation"]
        ]

        # null fields - list of fields which are not present in the fact tables which have
        # to be in the entity table as a column
        extra_fields = [
            "entity",
            "dataset",
            "typology",
            "json",
            "organisation_entity",
            "organisation",
        ]
        null_fields = [
            field
            for field in entity_fields
            if field not in (distinct_fields + extra_fields)
        ]

        # select fields - a list  of fields which have to be selected directly from the pivoted table
        # these are entity fields that are not null fields or a few special ones
        extra_fields = [
            "json",
            "organisation_entity",
            "dataset",
            "typology",
            "organisation",
        ]
        select_fields = [
            field for field in entity_fields if field not in null_fields + extra_fields
        ]

        # set fields
        fields_to_include = ["entity", "field", "value"]
        fields_str = ", ".join(fields_to_include)

        # Take original data, group by entity & field, and order by highest priority then latest record.
        # If there are still matches then pick the first resource (and fact, just to make sure)
        query = f"""
            SELECT {fields_str}
            FROM temp_table
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY entity, field
                ORDER BY priority, "entry-date" DESC, "entry-number" DESC, resource, fact
            ) = 1
        """

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
        select_statement = ", ".join([f"t1.{field}" for field in select_fields])
        # Don't want to include anything that ends with "_geom"
        null_fields_statement = ", ".join(
            [
                f"''::VARCHAR AS \"{field}\""
                for field in null_fields
                if not field.endswith("_geom")
            ]
        )
        json_statement = ", ".join(
            [
                f"CASE WHEN t1.{field} IS NOT NULL THEN REPLACE('{field}', '_', '-') ELSE NULL END, t1.{field}"
                for field in json_fields
            ]
        )

        # define organisation query
        org_csv = organisation_path
        org_query = f"""
             SELECT * FROM read_csv_auto('{org_csv}')
         """

        sql = f"""
            INSTALL spatial; LOAD spatial;
            COPY(
                WITH computed_centroid AS (
                    SELECT
                        * EXCLUDE (point), -- Calculate centroid point if not given
                        CASE
                            WHEN (geometry IS NOT NULL and geometry <> '') AND (point IS NULL OR point = '')
                            THEN ST_AsText(ST_ReducePrecision(ST_Centroid(ST_GeomFromText(geometry)),0.000001))
                            ELSE point
                        END AS point
                    FROM (
                        SELECT '{self.dataset}' as dataset,
                        '{self.typology}' as typology,
                        t2.entity as organisation_entity,
                        {select_statement},
                        {null_fields_statement},
                        json_object({json_statement}) as json,
                        FROM ({pivot_query}) as t1
                        LEFT JOIN ({org_query}) as t2
                        on t1.organisation = t2.organisation
                        )
                    )
                SELECT
                    * EXCLUDE (json),
                    CASE WHEN json = '{{}}' THEN NULL ELSE json END AS json
                FROM computed_centroid
            ) TO '{self.cache_dir}/entity{self.suffix}' (FORMAT PARQUET);
         """
        self.conn.execute(sql)

    def pq_to_sqlite(self):
        # At present we are saving the parquet files in 'cache' but saving the sqlite files produced in 'dataset'
        # In future when parquet files are saved to 'dataset' remove the 'cache_dir' in the function arguments and
        # replace 'cache_dir' with 'output_path' in this function's code
        logging.info(
            f"loading sqlite3 tables in {self.path} from parquet files in {self.cache_dir}"
        )
        query = "INSTALL sqlite; LOAD sqlite;"
        self.conn.execute(query)

        parquet_files = [
            fn for fn in os.listdir(self.cache_dir) if fn.endswith(self.suffix)
        ]

        for parquet_file in parquet_files:
            table_name = os.path.splitext(os.path.basename(parquet_file))[0]

            # Load Parquet data into DuckDB temp table
            self.conn.execute("DROP TABLE IF EXISTS temp_table;")
            self.conn.execute(
                f"""
                CREATE TABLE temp_table AS
                SELECT * FROM parquet_scan('{self.cache_dir}/{parquet_file}');
                """
            )

            # Export the DuckDB table to the SQLite database
            self.conn.execute(
                f"ATTACH DATABASE '{self.path}' AS sqlite_db (TYPE SQLITE);"
            )

            # Fix the column names
            for column in self.conn.execute("DESCRIBE TABLE temp_table;").fetchall():
                if "-" in column[0]:
                    self.conn.execute(
                        f"ALTER TABLE temp_table RENAME COLUMN '{column[0]}' TO '{column[0].replace('-','_')}';"
                    )

            # Copy the data
            self.conn.execute(
                f"INSERT INTO sqlite_db.{table_name} BY NAME (SELECT * FROM temp_table);"
            )

            self.conn.execute("DETACH DATABASE sqlite_db;")

    def close_conn(self):
        logging.info("Close connection to duckdb database in session")
        if self.conn is not None:
            self.conn.close()
            if os.path.exists(self.duckdb_file):
                os.remove(self.duckdb_file)

    def load(self):
        pass
