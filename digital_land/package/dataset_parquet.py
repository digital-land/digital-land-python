import os
import logging
import duckdb
import shutil
from pathlib import Path
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
    def __init__(self, dataset, path, duckdb_path=None, **kwargs):
        """
        Initialisation method to set up information as needed

        args:
            dataset (str): name of the dataset
            dir (str): the directory to store the package in
            duckdb_path (str): optional parameter to use  a duckdb file instead of in memory db
        """
        # this is a given at this point to not  sure  we need it the base package class might use this
        self.suffix = ".parquet"
        super().__init__(dataset, tables=tables, indexes=indexes, path=path, **kwargs)
        self.dataset = dataset
        # self.cache_dir = cache_dir
        # Persistent connection for the class. Given name to ensure that table is stored on disk (not purely in memory)
        if duckdb_path is not None:
            self.duckdb_path = Path(duckdb_path)
            self.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
            self.conn = duckdb.connect(self.duckdb_path)
        else:
            self.conn = duckdb.connect()

        self.schema = self.get_schema()
        self.typology = self.specification.schema[dataset]["typology"]

        # set up key file paths
        self.fact_path = self.path / "fact" / f"dataset={self.dataset}" / "fact.parquet"
        self.fact_resource_path = (
            self.path
            / "fact-resource"
            / f"dataset={self.dataset}"
            / "fact-resource.parquet"
        )
        self.entity_path = (
            self.path / "entity" / f"dataset={self.dataset}" / "entity.parquet"
        )

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

    # will be removed as we will remove the temp table from this logic
    # def create_temp_table(self, input_paths):
    #     # Create a temp table of the data from input_paths as we need the information stored there at various times
    #     logging.info(
    #         f"loading data into temp table from {os.path.dirname(input_paths[0])}"
    #     )

    #     input_paths_str = ", ".join([f"'{path}'" for path in input_paths])

    #     # Initial max_line_size and increment step
    #     max_size = 40000000
    #     # increment_step = 20000000
    #     # max_limit = 200000000  # Maximum allowable line size to attempt

    #     # increment = False
    #     while True:
    #         try:
    #             self.conn.execute("DROP TABLE IF EXISTS temp_table")
    #             query = f"""
    #                 CREATE TEMPORARY TABLE temp_table AS
    #                 SELECT *
    #                 FROM read_csv(
    #                     [{input_paths_str}],
    #                     columns = {self.schema},
    #                     header = true,
    #                     force_not_null = {[field for field in self.schema.keys()]},
    #                     max_line_size={max_size}
    #                 )
    #             """
    #     self.conn.execute(query)
    #     break
    # except duckdb.Error as e:  # Catch specific DuckDB error
    #     if "Value with unterminated quote" in str(e):
    #         hard_limit = int(resource.getrlimit(resource.RLIMIT_AS)[1])
    #         if max_size < hard_limit / 3:
    #             logging.info(
    #                 f"Initial max_size did not work, setting it to {hard_limit / 2}"
    #             )
    #             max_size = hard_limit / 2
    #         else:
    #             raise
    #     else:
    #         logging.info(f"Failed to read in when max_size = {max_size}")
    #         raise

    def load_facts(self, transformed_parquet_dir):
        """
        This method loads facts into a fact table from a directory containing all transformed files as parquet files
        """
        output_path = self.fact_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        logging.info("loading facts from temp table")

        fact_fields = self.specification.schema["fact"]["fields"]
        fields_str = ", ".join([field.replace("-", "_") for field in fact_fields])

        # query to extract data from the temp table (containing raw data), group by a fact, and get the highest
        # priority or latest record

        query = f"""
            SELECT {fields_str}
            FROM '{str(transformed_parquet_dir)}/*.parquet'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY fact ORDER BY priority, entry_date DESC, entry_number DESC
            ) = 1
        """
        self.conn.execute(
            f"""
            COPY (
                {query}
            ) TO '{str(output_path)}' (FORMAT PARQUET);
        """
        )

    def load_fact_resource(self, transformed_parquet_dir):
        logging.info(f"loading fact resources from {str(transformed_parquet_dir)}")
        output_path = self.fact_resource_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fact_resource_fields = self.specification.schema["fact-resource"]["fields"]
        fields_str = ", ".join(
            [field.replace("-", "_") for field in fact_resource_fields]
        )

        # All CSV files have been loaded into a temporary table. Extract several columns and export
        query = f"""
            SELECT {fields_str}
            FROM '{str(transformed_parquet_dir)}/*.parquet'
        """

        self.conn.execute(
            f"""
            COPY (
                {query}
            ) TO '{str(output_path)}' (FORMAT PARQUET);
        """
        )

    def load_entities_range(
        self,
        transformed_parquet_dir,
        resource_path,
        organisation_path,
        output_path,
        entity_range=None,
    ):
        # figure  out which resources we actually need to do  expensive queries on, store  in parquet
        # sql = f"""
        # COPY(
        #     SELECT DISTINCT resource
        #     FROM parquet_scan('{transformed_parquet_dir}/*.parquet')
        #     QUALIFY ROW_NUMBER() OVER (
        #         PARTITION BY enttity,field
        #         ORDER BY prioity, enttry_date DESC, entry_number DESC, resource, fact
        #         ) = 1
        #     ) TO '{self.cache_path / 'duckdb_temp_files' / 'distinct_resource.parquet'}' (FORMAT PARQUET);
        # """

        logger.info(f"loading entities from {transformed_parquet_dir}")

        entity_fields = self.specification.schema["entity"]["fields"]
        # Do this to match with later field names.
        entity_fields = [e.replace("-", "_") for e in entity_fields]
        # input_paths_str = f"{self.cache_dir}/fact{self.suffix}"
        if entity_range is not None:
            entity_where_clause = (
                f"WHERE entity >= {entity_range[0]} AND entity < {entity_range[1]}"
            )
        else:
            entity_where_clause = ""

        query = f"""
            SELECT DISTINCT REPLACE(field,'-','_')
            FROM parquet_scan('{transformed_parquet_dir}/*.parquet')
            {entity_where_clause}
        """

        # distinct_fields - list of fields in the field in fact
        rows = self.conn.execute(query).fetchall()
        distinct_fields = [row[0] for row in rows]

        # json fields - list of fields which are present in the fact table which
        # do not exist separately in the entity table so need to be included in the json field
        # Need to ensure that 'organisation' is not included either so  that it  is  excluded
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

        if "organisation" not in distinct_fields:
            null_fields.append("organisation")

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

        # create this statement to add a nul org  column, this is needed when no entities have an associated organisation
        if "organisation" not in distinct_fields:
            optional_org_str = ",''::VARCHAR AS \"organisation\""
        else:
            optional_org_str = ""

        # Take original data, group by entity & field, and order by highest priority then latest record.
        # If there are still matches then pick the first resource (and fact, just to make sure)
        # changes to make
        # not sure why this is bringing a raw resourcce AND the temp_table this data is essentially the same
        # need the resource hash and entry number of the file, this is important for ordering
        # between these two, the onlly other metric that isn't in the factt resource table is the start date of the resource
        # query to get this info
        # query to use this info to get the most recent facts
        # query to turn the most recent facts into a pivot
        # query to sort the final table
        # query  to create the file

        # craft a where clause to limit entities in quetion, this chunking helps solve memory issues

        query = f"""
            SELECT {fields_str}{optional_org_str} FROM (
                SELECT {fields_str}, CASE WHEN resource_csv."end-date" IS NULL THEN '2999-12-31' ELSE resource_csv."end-date" END AS resource_end_date
                FROM parquet_scan('{transformed_parquet_dir}/*.parquet') tf
                LEFT JOIN read_csv_auto('{resource_path}', max_line_size=40000000) resource_csv
                ON tf.resource = resource_csv.resource
                {entity_where_clause}
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY entity, field
                    ORDER BY priority, entry_date DESC, entry_number DESC, resource_end_date DESC, tf.resource, fact
                ) = 1
            )
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
             SELECT * FROM read_csv_auto('{org_csv}', max_line_size=40000000)
         """

        # should installinng spatial be done here
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
            ) TO '{str(output_path)}' (FORMAT PARQUET);
         """
        #  might  need  to un some fetch all toget result back
        self.conn.execute(sql)

    def combine_parquet_files(self, input_path, output_path):
        """
        This method combines multiple parquet files into a single parquet file
        """
        logger.info(f"combining parquet files from {input_path} into {output_path}")
        # use self.conn to use  duckdb to combine files
        sql = f"""
            COPY (select * from parquet_scan('{input_path}/*.parquet')) TO '{output_path}' (FORMAT PARQUET);
        """
        self.conn.execute(sql)

    def load_entities(self, transformed_parquet_dir, resource_path, organisation_path):
        output_path = self.entity_path
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # retrieve entity counnts including and minimum
        min_sql = f"select MIN(entity) FROM parquet_scan('{transformed_parquet_dir}/*.parquet');"
        min_entity = self.conn.execute(min_sql).fetchone()[0]
        max_sql = f"select MAX(entity) FROM parquet_scan('{transformed_parquet_dir}/*.parquet');"
        max_entity = self.conn.execute(max_sql).fetchone()[0]
        total_entities = max_entity - min_entity
        entity_limit = 1000000
        if total_entities > entity_limit:
            # create a temparary output path to store separate entity file in
            temp_dir = (
                output_path.parent
                / "temp_parquet_files"
                / "title-boundaries"
                / "entity_files"
            )
            temp_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"total entities {total_entities} exceeds limit {entity_limit}")
            _ = min_entity
            file_count = 1
            while _ < max_entity:
                temp_output_path = temp_dir / f"entity_{file_count}.parquet"
                entity_range = [_, _ + entity_limit]
                logger.info(
                    f"loading entities from {entity_range[0]} to {entity_range[1]}"
                )
                self.load_entities_range(
                    transformed_parquet_dir,
                    resource_path,
                    organisation_path,
                    temp_output_path,
                    entity_range,
                )
                _ += entity_limit
                file_count += 1
            # combine all the parquet files into a single parquet file
            self.combine_parquet_files(temp_dir, output_path)

            # remove temporary files
            shutil.rmtree(temp_dir)
        else:
            self.load_entities_range(
                transformed_parquet_dir, resource_path, organisation_path, output_path
            )

    def load_to_sqlite(self, sqlite_path):
        """
        Convert parquet files to sqlite3 tables assumes the sqlite table already exist. There is an arguement to
        say we want to improve the loading functionality of a sqlite package
        """
        # At present we are saving the parquet files in 'cache' but saving the sqlite files produced in 'dataset'
        # In future when parquet files are saved to 'dataset' remove the 'cache_dir' in the function arguments and
        # replace 'cache_dir' with 'output_path' in this function's code
        logger.info(
            f"loading sqlite3 tables in {sqlite_path} from parquet files in {self.path}"
        )
        # migrate to connection creation
        query = "INSTALL sqlite; LOAD sqlite;"
        self.conn.execute(query)

        # attache the sqlite db to duckdb
        self.conn.execute(
            f"ATTACH DATABASE '{sqlite_path}' AS sqlite_db (TYPE SQLITE);"
        )

        fact_resource_fields = self.specification.schema["fact-resource"]["fields"]
        fields_str = ", ".join(
            [field.replace("-", "_") for field in fact_resource_fields]
        )

        logger.info("loading fact_resource data")
        # insert fact_resource data
        self.conn.execute(
            f"""
                INSERT INTO sqlite_db.fact_resource
                SELECT {fields_str} FROM parquet_scan('{self.fact_resource_path}')
            """
        )

        logger.info("loading fact data")
        # insert fact data
        fact_fields = self.specification.schema["fact"]["fields"]
        fields_str = ", ".join([field.replace("-", "_") for field in fact_fields])

        self.conn.execute(
            f"""
                INSERT INTO sqlite_db.fact
                SELECT {fields_str} FROM parquet_scan('{self.fact_path}')
            """
        )

        logger.info("loading entity data")
        # insert entity data
        entity_fields = self.specification.schema["entity"]["fields"]
        fields_str = ", ".join(
            [
                field.replace("-", "_")
                for field in entity_fields
                if field not in ["geometry-geom", "point-geom"]
            ]
        )
        self.conn.execute(
            f"""
                INSERT INTO sqlite_db.entity
                SELECT {fields_str} FROM parquet_scan('{self.entity_path}')
            """
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
