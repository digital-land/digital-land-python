import os
import logging
import sys

import duckdb
import shutil
from pathlib import Path
from .package import Package
import psutil

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
    def __init__(
        self, dataset, path, duckdb_path=None, transformed_parquet_dir=None, **kwargs
    ):
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
        if transformed_parquet_dir is None:
            self.strategy = "direct"
        else:
            self.parquet_dir_details = self.analyze_parquet_dir(
                transformed_parquet_dir=transformed_parquet_dir
            )
            self.strategy = self.choose_strategy(self.parquet_dir_details)

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

    def analyze_parquet_dir(self, transformed_parquet_dir):
        """
        Get details about the transformed_parquet_dir to decide on which strategy to use for
        creating the fact and fact_resource tables
        """
        files = list(transformed_parquet_dir.glob("*.parquet"))
        no_parquet_files = len(files)
        total_size_bytes = sum(f.stat().st_size for f in files)
        total_size_mb = total_size_bytes / (1024 * 1024)
        avg_size_mb = total_size_mb / len(files) if files else 0
        max_size_mb = max(f.stat().st_size for f in files) / (1024 * 1024)
        mem = psutil.virtual_memory()
        memory_available = mem.total / 1024**2

        return {
            "no_parquet_files": no_parquet_files,
            "total_size_mb": total_size_mb,
            "avg_size_mb": avg_size_mb,
            "max_size_mb": max_size_mb,
            "memory_available": memory_available,
        }

    def choose_strategy(self, parquet_dir_details):
        """
        What strategy should we use to create fact, fact_resource and entity tables:
        Return one of:
        - "direct" - analyse all parquet files at once
        - "batch" - group the parquet files into batch files of approx. 256MB
        - Did have the other following as potential strategies but it appears as if 'batch' and 'direct' will suffice
        - since batching everything into one fie is the equivalent os the 'single_file' option.
        - "single_file" - put all parquet files into a single parquet file
        - "consolidate_then_bucket" - put all parquet files into several larger files
        """
        # If memory is less than 2GB (or 1/4 of available memory, whichever is smaller) then can potentially process
        # them directly
        memory_check = min(2048, parquet_dir_details["memory_available"] / 4)
        if (
            parquet_dir_details["total_size_mb"] < memory_check
            and parquet_dir_details["no_parquet_files"] < 100
        ) or (parquet_dir_details["no_parquet_files"] < 4):
            return "direct"

        # if parquet_dir_details["no_parquet_files"] > 500 or parquet_dir_details[
        #     "total_size_mb"
        # ] > (parquet_dir_details["memory_available"] * 0.75):
        #     return "consolidate_then_bucket"

        return "batch"

    def group_parquet_files(
        self, transformed_parquet_dir, target_mb=256, delete_originals=False
    ):
        """
        group parquet files into batches, each aiming for approximately 'target_mb' in size.
        """
        process = psutil.Process(os.getpid())
        mem = process.memory_info().rss / 1024**2  # Memory in MB
        logger.info(f"[Memory usage] Before grouping: {mem:.2f} MB")

        logger.info(f"Batching all files from {str(transformed_parquet_dir)}")
        target_bytes = target_mb * 1024 * 1024
        parquet_files = list(transformed_parquet_dir.glob("*.parquet"))

        # List of (file_path, file_size_in_bytes)
        file_sizes = [(f, f.stat().st_size) for f in parquet_files]
        file_sizes.sort(key=lambda x: x[1], reverse=True)

        batches = []

        # apply first-fit decreasing heuristic
        for f, size in file_sizes:
            placed = False
            for batch in batches:
                if batch["total_size"] + size <= target_bytes:
                    batch["files"].append(f)
                    batch["total_size"] += size
                    placed = True
                    break
            if not placed:
                # Start a new batch
                batches.append({"files": [f], "total_size": size})

        digits = max(2, len(str(len(batches) - 1)))
        batch_dir = transformed_parquet_dir / "batch"
        batch_dir.mkdir(parents=True, exist_ok=True)
        for i, batch in enumerate(batches):
            files = batch["files"]
            output_file = batch_dir / f"batch_{i:0{digits}}.parquet"
            files_str = ", ".join(f"'{str(f)}'" for f in files)
            query = f"""
                COPY (
                    SELECT * FROM read_parquet([{files_str}])
                ) TO '{str(output_file)}' (FORMAT PARQUET)
            """
            self.conn.execute(query)

            # Should we delete the files now that they have been 'batched'?
            if delete_originals:
                for f in files:
                    f.unlink()

        process = psutil.Process(os.getpid())
        mem = process.memory_info().rss / 1024**2  # Memory in MB
        logger.info(f"[Memory usage] After grouping: {mem:.2f} MB")

    def load_facts(self, transformed_parquet_dir):
        """
        This method loads facts into a fact table from a directory containing all transformed files as parquet files
        """
        output_path = self.fact_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"loading facts from from {str(transformed_parquet_dir)}")

        fact_fields = self.specification.schema["fact"]["fields"]
        fields_str = ", ".join([field.replace("-", "_") for field in fact_fields])

        process = psutil.Process(os.getpid())
        mem = process.memory_info().rss / 1024**2  # Memory in MB
        logger.info(f"[Memory usage] At start of load_facts: {mem:.2f} MB")

        # query to extract data from either the transformed parquet files or batched ones. Data is grouped by fact,
        # and we get the highest priority or latest record
        if self.strategy == "direct":
            logger.info("Using direct strategy for facts")
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
        else:
            no_of_batched_files = len(
                list(transformed_parquet_dir.glob("batch/batch_*.parquet"))
            )
            if no_of_batched_files == 1:
                logger.info(
                    "Only have one batched file for facts - using 'simple' query"
                )
                n_buckets = 1
            else:
                # Max partition size should the smallest value of either be 2GB or 1/4 of the available memory
                max_partition_memory_mb = min(
                    2048, self.parquet_dir_details["memory_available"] / 4
                )
                n_buckets = (
                    self.parquet_dir_details["total_size_mb"] // max_partition_memory_mb
                    + 1
                )
                n_buckets = int(min(n_buckets, no_of_batched_files))
                if n_buckets == 0:
                    sys.exit(
                        "Have got a value of zero for n_buckets in `load_facts`. Cannot continue."
                    )
                if n_buckets == 1:
                    logger.info("Only need one bucket for facts - using 'simple' query")
            if no_of_batched_files == 1 or n_buckets == 1:
                query = f"""
                    SELECT {fields_str}
                    FROM '{str(transformed_parquet_dir)}/batch/batch_*.parquet'
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
            else:
                # Multiple buckets and batched files used
                logger.info(
                    "Need to use multiple buckets in windowed function for facts"
                )
                bucket_dir = transformed_parquet_dir / "bucket"
                bucket_dir.mkdir(parents=True, exist_ok=True)
                digits = max(2, len(str(n_buckets - 1)))
                bucket_paths = [
                    bucket_dir / f"bucket_{i:0{digits}}.parquet"
                    for i in range(n_buckets)
                ]
                logger.info(
                    f"Have {len(list(transformed_parquet_dir.glob('batch/batch_*.parquet')))} batch files"
                )

                # Loop over each batch file and assign to a bucket file
                logger.info(f"Assigning to {n_buckets} buckets")
                for f in transformed_parquet_dir.glob("batch/batch_*.parquet"):
                    for i in range(n_buckets):
                        self.conn.execute(
                            f"""
                        COPY (
                            SELECT *
                            FROM read_parquet('{f}')
                            WHERE MOD(HASH(fact), {n_buckets}) = {i}
                        ) TO '{bucket_paths[i]}' (FORMAT PARQUET, APPEND TRUE);
                        """
                        )

                logger.info(
                    f"Have {len(list(bucket_dir.glob('bucket_*.parquet')))} bucket files"
                )
                process = psutil.Process(os.getpid())
                mem = process.memory_info().rss / 1024**2  # Memory in MB
                logger.info(f"[Memory usage] After 'bucketing': {mem:.2f} MB")

                result_dir = transformed_parquet_dir / "result"
                result_dir.mkdir(parents=True, exist_ok=True)
                result_paths = [
                    result_dir / f"result_{i:0{digits}}.parquet"
                    for i in range(n_buckets)
                ]
                for i in range(n_buckets):
                    bucket_path = bucket_dir / f"bucket_{i:0{digits}}.parquet"
                    self.conn.execute(
                        f"""
                    COPY (
                        SELECT *
                        FROM read_parquet('{bucket_path}')
                        QUALIFY ROW_NUMBER() OVER (
                            PARTITION BY fact ORDER BY priority, entry_date DESC, entry_number DESC
                        ) = 1
                    ) TO '{result_paths[i]}' (FORMAT PARQUET);
                    """
                    )
                logger.info(
                    f"Have {len(list(transformed_parquet_dir.glob('result_*.parquet')))} result files"
                )

                # for path in bucket_paths:
                #     path.unlink(missing_ok=True)

                process = psutil.Process(os.getpid())
                mem = process.memory_info().rss / 1024**2  # Memory in MB
                logger.info(f"[Memory usage] After 'result': {mem:.2f} MB")

                self.conn.execute(
                    f"""
                    COPY(
                        SELECT * FROM
                    read_parquet('{str(result_dir)}/result_*.parquet')
                ) TO '{str(output_path)}' (FORMAT PARQUET);
                """
                )
                logger.info(f"Facts saved to output_path: {output_path}")
                logger.info(f"output_path exists: {os.path.exists(output_path)}")
                # for path in result_paths:
                #     path.unlink(missing_ok=True)

            process = psutil.Process(os.getpid())
            mem = process.memory_info().rss / 1024**2  # Memory in MB
            logger.info(f"[Memory usage] At end of query: {mem:.2f} MB")

    def load_fact_resource(self, transformed_parquet_dir):
        logger.info(f"loading fact resources from {str(transformed_parquet_dir)}")
        output_path = self.fact_resource_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fact_resource_fields = self.specification.schema["fact-resource"]["fields"]
        fields_str = ", ".join(
            [field.replace("-", "_") for field in fact_resource_fields]
        )

        process = psutil.Process(os.getpid())
        mem = process.memory_info().rss / 1024**2  # Memory in MB
        logger.info(f"[Memory usage] At start: {mem:.2f} MB")

        # Extract relevant columns from original parquet or batched parquet files
        if self.strategy == "direct":
            parquet_str = "*.parquet"
        else:
            parquet_str = "batch/batch_*.parquet"
        query = f"""
            SELECT {fields_str}
            FROM '{str(transformed_parquet_dir)}/{parquet_str}'
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

        if self.strategy == "direct":
            parquet_str = "*.parquet"
        else:
            parquet_str = "batch/batch_*.parquet"
        query = f"""
            SELECT DISTINCT REPLACE(field,'-','_')
            FROM parquet_scan('{transformed_parquet_dir}/{parquet_str}')
            {entity_where_clause}
        """

        # distinct_fields - list of fields in the field in fact
        rows = self.conn.execute(query).fetchall()
        # if there are no entities in the entity range then we don't need to proceed
        if len(rows) == 0:
            logger.info(
                f"skipping entity numbers {entity_range[0]} to {entity_range[1]} as there are no entities"
            )
            return

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

        if self.strategy == "direct":
            parquet_str = "*.parquet"
        else:
            parquet_str = "batch/batch_*.parquet"

        query = f"""
            SELECT {fields_str}{optional_org_str} FROM (
                SELECT {fields_str}, CASE WHEN resource_csv."end-date" IS NULL THEN '2999-12-31' ELSE resource_csv."end-date" END AS resource_end_date
                FROM parquet_scan('{transformed_parquet_dir}/{parquet_str}') tf
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
        if self.strategy == "direct":
            parquet_str = "*.parquet"
        else:
            parquet_str = "batch/batch_*.parquet"
        min_sql = f"select MIN(entity) FROM parquet_scan('{transformed_parquet_dir}/{parquet_str}');"
        min_entity = self.conn.execute(min_sql).fetchone()[0]
        max_sql = f"select MAX(entity) FROM parquet_scan('{transformed_parquet_dir}/{parquet_str}');"
        max_entity = self.conn.execute(max_sql).fetchone()[0]
        total_entities = max_entity - min_entity
        entity_limit = 100000
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
                transformed_parquet_dir,
                resource_path,
                organisation_path,
                output_path,
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
        logger.info(self.fact_path)
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
