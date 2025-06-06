import uuid
import csv

from pathlib import Path

from .postgres import PostgresPackage


class PlatformPackage(PostgresPackage):
    """
    A very specific class used to represent the postgis database behind our platform. Primarily for loading of data into it.

    This class inherits from PostgresPackage and is used to handle the specific requirements of the data in the platform including
    how data can be absorbed and loaded from other sources
    """

    def __init__(
        self,
        database_url=None,
        conn=None,
    ):
        self.database_url = database_url
        self.conn = conn

    @staticmethod
    def _reorder_field_types(csv_path: Path, field_types: dict) -> dict:
        """
        helper Function to reorder the field types based on headers form the csv
        validates the corect values are there as well
        """
        with open(csv_path, newline="") as csvfile:
            reader = csv.DictReader(csvfile, delimiter="|")
            header = reader.fieldnames

        # Step 2: Check if headers match the dict keys
        csv_keys_set = set(header)
        dict_keys_set = set(field_types.keys())

        if csv_keys_set != dict_keys_set:
            missing_in_csv = dict_keys_set - csv_keys_set
            extra_in_csv = csv_keys_set - dict_keys_set
            raise ValueError(
                f"CSV headers do not match expected fields.\n"
                f"Missing: {missing_in_csv}\n"
                f"Extra: {extra_in_csv}"
            )

        # Step 3: Reorder dict based on CSV header
        ordered_field_types = {key: field_types[key] for key in header}
        return ordered_field_types

    def update_fact_resource(self, input_file: Path, conn=None):
        """
        Function to update the fact resource table from a csv
        """

        if not conn:
            conn = self._get_conn()

        temp_table = f"temp_import_{uuid.uuid4().hex}"
        # load csv into a temporary table in postgis
        # remove rows from fact resource where the resource is in the new data
        # add the rows to  the fact resouce table
        # remove the temporary table
        fact_resource_fields = {
            "fact": "VARCHAR(64)",
            "resource": "VARCHAR(64)",
            "entry_date": "DATE",
            "entry_number": "INTEGER",
            "priority": "INTEGER",
            "reference_entity": "BIGINT",
            "dataset": "VARCHAR(64)",
        }

        non_key_fields = {
            "reference_entity",
        }

        fact_resource_fields = self._reorder_field_types(
            field_types=fact_resource_fields, csv_path=input_file
        )

        with conn.cursor() as cur:
            # need to get list of table from spec so the specification is needed
            # 1. Create TEMPORARY TABLE
            cur.execute(
                f"""
                CREATE TEMPORARY TABLE {temp_table} (
                    {','.join([f'{k} {v}' for k, v in fact_resource_fields.items()])}
                ) ON COMMIT DROP;
            """
            )

            # 2. Use COPY to load data from CSV (very fast, streaming)
            with open(input_file, "r") as f:
                cur.copy_expert(
                    f"""
                    COPY {temp_table} ({','.join([f'{k}' for k in fact_resource_fields.keys()])})
                    FROM STDIN WITH (FORMAT csv, HEADER, DELIMITER '|', FORCE_NULL(
                        {",".join(non_key_fields)}
                    ));
                """,
                    f,
                )

            # 3. Delete matching rows from main table
            cur.execute(
                f"""
                DELETE FROM fact_resource
                WHERE resource IN (SELECT resource FROM {temp_table});
            """
            )

            # 4. Insert new rows from temp table into main table
            cur.execute(
                f"""
                INSERT INTO fact_resource ({','.join([f'{k}' for k in fact_resource_fields.keys()])})
                SELECT {','.join([f'{k}' for k in fact_resource_fields.keys()])} FROM {temp_table};
            """
            )

    def update_fact(self, input_file: Path, conn=None):
        """
        Method to update the fact table. It's worth noting that the fact_resource table should be updated ahead of this
        """

        if not conn:
            conn = self._get_conn()

        temp_table = f"temp_import_{uuid.uuid4().hex}"
        # load csv into a temporary table in postgis
        # remove rows from fact resource where the resource is in the new data
        # add the rows to  the fact resouce table
        # remove the temporary table
        # TODO how  can  the  schema be flexible
        field_types = {
            "fact": "VARCHAR(64)",
            "entity": "BIGINT",
            "field": "VARCHAR(64)",
            "value": "TEXT",
            "resource": "VARCHAR(64)",
            "entry_date": "DATE",
            "entry_number": "INTEGER",
            "priority": "INTEGER",
            "reference_entity": "BIGINT",
            "dataset": "VARCHAR(64)",
        }

        non_key_fields = {
            "reference_entity",
        }

        # make sure the ode of the the field types match the csv
        field_types = self._reorder_field_types(
            field_types=field_types, csv_path=input_file
        )

        with conn.cursor() as cur:
            # 1. Create TEMPORARY TABLE
            cur.execute(
                f"""
                CREATE TEMPORARY TABLE {temp_table} (
                    {','.join([f'{k} {v}' for k, v in field_types.items()])}
                ) ON COMMIT DROP;
            """
            )

            # 2. Use COPY to load data from CSV (very fast, streaming)
            with open(input_file, "r") as f:
                cur.copy_expert(
                    f"""
                    COPY {temp_table} ({','.join([f'{k}' for k in field_types.keys()])})
                    FROM STDIN WITH (FORMAT csv, HEADER, DELIMITER '|', FORCE_NULL(
                        {",".join(non_key_fields)}
                    ));
                """,
                    f,
                )

            # could replace the 3rd condition with the resourcce entry-date
            cur.execute(
                f"""
                INSERT INTO fact (fact,entity,field,value, entry_number, resource, entry_date, priority,reference_entity, dataset)
                SELECT fact,entity,field,value, entry_number, resource, entry_date, priority, reference_entity, dataset
                FROM {temp_table}
                ON CONFLICT (fact) DO UPDATE
                SET
                    entry_number = EXCLUDED.entry_number,
                    resource = EXCLUDED.resource,
                    entry_date = EXCLUDED.entry_date,
                    priority = EXCLUDED.priority,
                    reference_entity = EXCLUDED.reference_entity,
                    dataset = EXCLUDED.dataset
                WHERE EXCLUDED.priority > fact.priority
                    OR (EXCLUDED.priority = fact.priority AND EXCLUDED.entry_date > fact.entry_date)
                    OR (EXCLUDED.priority = fact.priority AND EXCLUDED.entry_date = fact.entry_date AND EXCLUDED.resource > fact.resource)
                    OR (
                        EXCLUDED.priority = fact.priority
                        AND EXCLUDED.entry_date = fact.entry_date
                        AND EXCLUDED.resource = fact.resource
                        AND EXCLUDED.entry_number > fact.entry_number
                    )
            """
            )

            # remove facts not in fact_resource
            cur.execute(
                """
                    DELETE FROM fact
                    WHERE fact NOT IN (SELECT fact FROM fact_resource);
                """
            )

            # update facts where fact resoure combo isn't in fact_rersource
            cur.execute(
                """
                    WITH update_facts AS (
                        SELECT f.fact
                        FROM fact f
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM fact_resource fr
                            WHERE fr.fact = f.fact AND fr.resource = f.resource
                        )
                    )
                    INSERT INTO fact (fact, entity, field, value, reference_entity, entry_number, resource, entry_date, priority, dataset)
                    SELECT t1.fact, t1.entity, t1.field, t1.value,  t1.reference_entity, t1.entry_number, t1.resource, t1.entry_date, t1.priority, t1.dataset
                    FROM (
                            SELECT DISTINCT ON (fr.fact)
                                fr.fact,
                                fr.entry_number,
                                fr.resource,
                                fr.entry_date,
                                fr.priority,
                                f.reference_entity,
                                f.entity,
                                f.field,
                                f.value,
                                f.dataset
                            FROM fact_resource fr
                            JOIN fact f on f.fact = fr.fact
                            ORDER BY fr.fact,fr.priority, fr.entry_date DESC,fr.resource, fr.entry_number DESC
                    ) AS t1
                    ON CONFLICT (fact) DO UPDATE
                    SET
                        entry_number = EXCLUDED.entry_number,
                        resource = EXCLUDED.resource,
                        entry_date = EXCLUDED.entry_date,
                        priority = EXCLUDED.priority,
                        reference_entity = EXCLUDED.reference_entity,
                        dataset = EXCLUDED.dataset
                """
            )

    def update_entity(self, entity_csv, conn):
        """
        Function tto update the entities from the fact and fact resource tables
        after updating the fact the and fact resource table Accepts a csv of entities
        which need updating (might want to rework this in the future)
        """
        if not conn:
            conn = self._get_conn()

        temp_table = f"temp_entity_{uuid.uuid4().hex}"
        # load csv into a temporary table in postgis
        # remove rows from fact resource where the resource is in the new data
        # add the rows to  the fact resouce table
        # remove the temporary table
        # TODO how  can  the  schema be flexible
        # field_types = {
        #     "entity": "BIGINT",
        #     "field": "VARCHAR(64)",
        #     "value": "TEXT",
        #     "dataset": "VARCHAR(64)",
        #     "entry_date": "DATE",
        #     "entry_number": "INTEGER",
        #     "priority": "INTEGER",
        #     "reference_entity": "BIGINT"
        # }

        # field_types = {
        #     "entity": "BIGINT (primary key, autoincrement=False)",
        #     "name": "Text (nullable=True)",
        #     "entry_date": "Date (nullable=True)",
        #     "start_date": "Date (nullable=True)",
        #     "end_date": "Date (nullable=True)",
        #     "dataset": "Text (nullable=True)",
        #     "json": "JSONB (nullable=True)",
        #     "organisation_entity": "BIGINT (nullable=True)",
        #     "prefix": "Text (nullable=True)",
        #     "reference": "Text (nullable=True)",
        #     "typology": "Text (nullable=True)",
        #     "geometry": "Geometry(MULTIPOLYGON, SRID=4326, nullable=True)",
        #     "point": "Geometry(POINT, SRID=4326, nullable=True)",
        #     "geojson_col": "JSONB (nullable=True, column name = 'geojson')",
        # }

        # non_key_fields = {
        #     "reference_entity",
        # }

        with conn.cursor() as cur:
            # I think we'e over complicatinng it with PIVOT functions
            # we are performaing a distinct set of calculations on entity goup to extract  information
            # from one column
            # pivoting is spilling everything for no reason

            # 1. Create TEMPORARY TABLE
            cur.execute(
                f"""
                CREATE TEMPORARY TABLE {temp_table} (
                    entity BIGINT PRIMARY KEY
                ) ON COMMIT DROP;
            """
            )

            with open(entity_csv, "r") as f:
                cur.copy_expert(
                    f"""
                    COPY {temp_table} (entity)
                    FROM STDIN WITH (FORMAT csv, HEADER, DELIMITER '|');
                """,
                    f,
                )

            # create disinct and pivot
            # case_fields = [
            #     "name",
            #     "start_date",
            #     "end_date",
            # ]
            cur.execute(
                f"""
                WITH latest_fact AS (
                    SELECT DISTINCT ON (field)
                        fact.*
                    FROM fact
                    JOIN {temp_table} temp on temp.entity = fact.entity
                    ORDER BY field, priority, entry_date DESC,resource ASC, entry_number DESC
                ),
                pre_entity AS (
                    SELECT entity,
                        MAX(CASE WHEN field = 'name' THEN value END) AS name,
                        MAX(CASE WHEN field = 'start_date' THEN value END) AS start_date,
                        MAX(CASE WHEN field = 'end_date' THEN value END) AS end_date,
                        MAX(CASE WHEN field = 'entry_date' THEN value END) AS entry_date,
                        MAX(dataset) AS dataset,
                        MAX(CASE WHEN field = 'organisation' THEN value END) AS organisation,
                        MAX(CASE WHEN field = 'prefix' THEN value END) AS prefix,
                        MAX(CASE WHEN field = 'reference' THEN value END) AS reference,
                        ST_SetSRID(
                            ST_GeomFromText(
                            MAX(CASE WHEN field = 'geometry' THEN value END)
                            ),
                            4326
                        ) AS geometry,
                        ST_SetSRID(
                            ST_GeomFromText(
                            MAX(CASE WHEN field = 'point' THEN value END)
                            ),
                            4326
                        ) AS point,
                        NULLIF(
                            jsonb_object_agg(field, value) FILTER (
                                WHERE field NOT IN ('name','start_date','end_date','dataset', 'reference', 'geometry')
                            ),
                            '{{}}'::jsonb
                        ) AS json
                    FROM latest_fact lf
                    GROUP BY entity
                )
                INSERT INTO entity (entity, start_date, end_date, entry_date,name,organisation_entity, dataset, prefix, reference, geometry, point, json)
                SELECT t1.entity,
                    t1.start_date,
                    t1.end_date,
                    t1.entry_date,
                    t1.name,
                    t2.entity as organisation_entity,
                    t1.dataset,
                    t1.prefix,
                    t1.reference,
                    t1.geometry,
                    t1.point,
                    t1.json
                FROM pre_entity t1
                LEFT JOIN organisation t2 on t2.organisation = t1.organisation
                ON CONFLICT (entity) DO UPDATE
                SET
                    start_date = EXCLUDED.start_date,
                    end_date = EXCLUDED.end_date,
                    entry_date = EXCLUDED.entry_date,
                    name = EXCLUDED.name,
                    organisation_entity = EXCLUDED.organisation_entity,
                    dataset = EXCLUDED.dataset,
                    prefix = EXCLUDED.prefix,
                    reference = EXCLUDED.reference,
                    geometry = EXCLUDED.geometry,
                    point = EXCLUDED.point,
                    json = EXCLUDED.json
            """
            )

            # remove any entities not in fact
            cur.execute(
                """
                    DELETE FROM entity
                    WHERE entity NOT IN (SELECT DISTINCT entity FROM fact);
                """
            )

    def update_issues(self, input_file, conn):
        """
        Function to update the issues table from a csv

        as this is updating not loading it will remove issues for any resources included in the update
        """

        if not conn:
            conn = self._get_conn()

        temp_table = f"temp_import_issue_{uuid.uuid4().hex}"
        # load csv into a temporary table in postgis
        # remove rows from fact resource where the resource is in the new data
        # add the rows to  the fact resouce table
        # remove the temporary table
        field_types = {
            "entity": "BIGINT",
            "entry_date": "DATE",
            "entry_number": "INTEGER",
            "field": "VARCHAR(64)",
            "issue_type": "VARCHAR(64)",
            "line_number": "INTEGER",
            "dataset": "VARCHAR(64)",  # also has a foreign key
            "resource": "VARCHAR(64)",
            "value": "TEXT",
            "message": "TEXT",
        }

        non_key_fields = {
            "entity",
            "entry_date",
            "value",
            "message",
        }

        field_types = self._reorder_field_types(
            field_types=field_types, csv_path=input_file
        )

        with conn.cursor() as cur:
            # need to get list of table from spec so the specification is needed
            # 1. Create TEMPORARY TABLE
            cur.execute(
                f"""
                CREATE TEMPORARY TABLE {temp_table} (
                    {','.join([f'{k} {v}' for k, v in field_types.items()])}
                ) ON COMMIT DROP;
            """
            )

            # 2. Use COPY to load data from CSV (very fast, streaming)
            with open(input_file, "r") as f:
                cur.copy_expert(
                    f"""
                    COPY {temp_table} ({','.join([f'{k}' for k in field_types.keys()])})
                    FROM STDIN WITH (FORMAT csv, HEADER, DELIMITER '|', FORCE_NULL(
                        {",".join(non_key_fields)}
                    ));
                """,
                    f,
                )

            # 3. Delete matching rows from main table
            cur.execute(
                f"""
                DELETE FROM issue
                WHERE resource IN (SELECT resource FROM {temp_table});
            """
            )

            # 4. Insert new rows from temp table into main table
            cur.execute(
                f"""
                INSERT INTO issue ({','.join([f'{k}' for k in field_types.keys()])})
                SELECT {','.join([f'{k}' for k in field_types.keys()])} FROM {temp_table};
            """
            )
