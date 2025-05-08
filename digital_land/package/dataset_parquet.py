import os
import logging
import duckdb
import json
import pandas as pd
from pathlib import Path
from .package import Package
from shapely import wkt
import dask.dataframe as dd

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


def combine_to_json_string(row):
    obj = {k: v for k, v in row.items() if pd.notnull(v)}
    return json.dumps(obj) if obj else None


def compute_point(row):
    if pd.isna(row["point"]) and pd.notna(row["geometry"]):
        return wkt.dumps(wkt.loads(row["geometry"]).centroid)
    else:
        return row["point"]


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

    def load_facts(self, transformed_parquet_dir):
        """
        This method loads facts into a fact table from a directory containing all transformed files as parquet files
        """
        output_path = self.fact_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"loading facts from from {str(transformed_parquet_dir)}")

        fact_fields = self.specification.schema["fact"]["fields"]
        # fields_str = ", ".join([field.replace("-", "_") for field in fact_fields])

        # # query to extract data from the temp table (containing raw data), group by a fact, and get the highest
        # # priority or latest record

        # query = f"""
        #     SELECT {fields_str}
        #     FROM '{str(transformed_parquet_dir)}/*.parquet'
        #     QUALIFY ROW_NUMBER() OVER (
        #         PARTITION BY fact ORDER BY priority, entry_date DESC, entry_number DESC
        #     ) = 1
        # """
        # self.conn.execute(
        #     f"""
        #     COPY (
        #         {query}
        #     ) TO '{str(output_path)}' (FORMAT PARQUET);
        # """
        # )
        # try  dask

        # Load a large CSV
        cols = [field.replace("-", "_") for field in fact_fields]
        cols.append("entry_number")
        df = dd.read_parquet(transformed_parquet_dir, columns=cols)

        df["dataset"] = self.dataset

        # Sort by the fields in order
        df_sorted = df.sort_values(["fact", "entry_date", "priority", "entry_number"])

        # Drop duplicates to keep the first row per record_id
        first_rows = df_sorted.drop_duplicates(subset="fact", keep="first")
        # Drop the entry_number column
        first_rows = first_rows.drop(columns=["entry_number"])

        first_rows.to_parquet(
            self.fact_path.parent.parent, partition_on=["dataset"], write_index=False
        )

    def load_fact_resource(self, transformed_parquet_dir):
        logger.info(f"loading fact resources from {str(transformed_parquet_dir)}")
        output_path = self.fact_resource_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fact_resource_fields = self.specification.schema["fact-resource"]["fields"]
        # fields_str = ", ".join(
        #     [field.replace("-", "_") for field in fact_resource_fields]
        # )

        # All CSV files have been loaded into a temporary table. Extract several columns and export
        # query = f"""
        #     SELECT {fields_str}
        #     FROM '{str(transformed_parquet_dir)}/*.parquet'
        # """

        # self.conn.execute(
        #     f"""
        #     COPY (
        #         {query}
        #     ) TO '{str(output_path)}' (FORMAT PARQUET);
        # """
        # )

        # Load a large CSV
        cols = [field.replace("-", "_") for field in fact_resource_fields]
        df = dd.read_parquet(transformed_parquet_dir, columns=cols)

        df["dataset"] = self.dataset
        # Sort by the fields in order

        df.to_parquet(
            self.fact_resource_path.parent.parent,
            partition_on=["dataset"],
            write_index=False,
        )

    def load_entities_range(
        self,
        transformed_parquet_dir,
        resource_path,
        organisation_path,
        output_path,
        entity_range=None,
    ):

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
        # select_fields = [
        #     field for field in entity_fields if field not in null_fields + extra_fields
        # ]

        # set fields
        # fields_to_include = ["entity", "field", "value"]

        df = dd.read_parquet(transformed_parquet_dir)
        df["field"] = df["field"].str.replace("-", "_")

        resource_df = dd.read_csv(resource_path, usecols=["resource", "end-date"])
        resource_df = resource_df.rename(columns={"end-date": "resource_end_date"})
        # if resource_end_date is null then set to 2999-12-31
        resource_df["resource_end_date"] = resource_df["resource_end_date"].fillna(
            "2999-12-31"
        )

        # join  resource on
        df = df.merge(resource_df, on="resource", how="left")

        # sort
        df = df.sort_values(
            [
                "entity",
                "field",
                "entry_date",
                "priority",
                "entry_number",
                "resource_end_date",
            ]
        )
        df = df.drop_duplicates(subset=["entity", "field"], keep="first")
        # drop the resource_end_date column
        df = df.drop(
            columns=[
                "resource_end_date",
                "end_date",
                "entry_date",
                "fact",
                "priority",
                "reference_entity",
                "resource",
                "start_date",
                "entry_number",
            ]
        )

        # nnow we have the data we need to pivot
        # pivot the data
        df["field"] = df["field"].astype("category").cat.set_categories(distinct_fields)
        df = df.pivot_table(
            index="entity", columns="field", values="value", aggfunc="first"
        )
        df = df.reset_index()

        # get org df
        org_df = dd.read_csv(organisation_path, usecols=["organisation", "entity"])
        org_df = org_df.rename(columns={"entity": "organisation_entity"})

        # join org or create org
        if "organisation" not in df.columns:
            df["organisation_entity"] = None
        else:
            df = df.merge(org_df, on="organisation", how="left")
            df = df.drop(columns=["organisation"])

        # create geometry if doesn't exist
        if "geometry" not in df.columns:
            df["geometry"] = None

        # create point if doesn't exist
        if "point" not in df.columns:
            df["point"] = None

        if "end_date" not in df.columns:
            df["end_date"] = ""

        if "entry_date" not in df.columns:
            df["entry_date"] = ""

        if "start_date" not in df.columns:
            df["start_date"] = ""

        if "name" not in df.columns:
            df["name"] = ""

        if "prefix" not in df.columns:
            df["prefix"] = ""

        if "reference" not in df.columns:
            df["reference"] = ""

        df["dataset"] = self.dataset
        df["typology"] = self.typology

        df["point"] = df.map_partitions(
            lambda partition: partition.apply(compute_point, axis=1),
            meta=("point", "object"),
        )

        # make json column
        df["json"] = df.map_partitions(
            lambda partition: partition[json_fields].apply(
                combine_to_json_string, axis=1
            ),
            meta=("json", "object"),
        )

        df = df.astype(
            {
                "entity": "int64",
                "entry_date": "string",
                "organisation_entity": "Int64",
                "geometry": "string",
                "point": "string",
                "end_date": "string",
                "start_date": "string",
                "name": "string",
                "prefix": "string",
                "reference": "string",
                "dataset": "string",
                "typology": "string",
                "json": "string",
            }
        )

        df.to_parquet(output_path, partition_on=["dataset"], write_index=False)

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
        self.load_entities_range(
            transformed_parquet_dir,
            resource_path,
            organisation_path,
            self.entity_path.parent.parent,
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
                SELECT {fields_str} FROM parquet_scan('{self.path / "fact-resource"}/*/*.parquet')
                WHERE dataset = '{self.dataset}'
            """
        )

        logger.info("loading fact data")
        # insert fact data
        fact_fields = self.specification.schema["fact"]["fields"]
        fields_str = ", ".join([field.replace("-", "_") for field in fact_fields])

        self.conn.execute(
            f"""
                INSERT INTO sqlite_db.fact
                SELECT {fields_str} FROM parquet_scan('{self.path /'fact'}/*/*.parquet')
                WHERE dataset = '{self.dataset}'
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
                SELECT {fields_str} FROM parquet_scan('{self.path / "entity"}/*/*.parquet')
                WHERE dataset = '{self.dataset}'
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
