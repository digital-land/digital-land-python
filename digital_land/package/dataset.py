import csv
import json
import logging
from decimal import Decimal

import shapely.wkt

from .sqlite import SqlitePackage, colname

logger = logging.getLogger(__name__)

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


class DatasetPackage(SqlitePackage):
    def __init__(self, dataset, organisation, **kwargs):
        super().__init__(dataset, tables=tables, indexes=indexes, **kwargs)
        self.dataset = dataset
        self.entity_fields = self.specification.schema["entity"]["fields"]
        self.organisations = organisation.organisation

    def migrate_entity(self, row):
        dataset = self.dataset
        entity = row.get("entity", "")

        if not entity:
            logging.error(f"{dataset} entity with a missing entity number")
            exit(1)

        row["dataset"] = dataset
        row["typology"] = self.specification.schema[dataset]["typology"]
        row["name"] = row.get("name", "")

        if not row.get("reference", ""):
            logging.error(f"entity {entity}: missing reference")

        # hack until FactReference is reliable ..
        if not row.get("organisation-entity", ""):
            row["organisation-entity"] = self.organisations.get(
                row.get("organisation", ""), {}
            ).get("entity", "")

        # extended fields as JSON properties
        if not row.get("json", ""):
            properties = {
                field: row[field]
                for field in row
                if row[field]
                and field
                not in [
                    "geography",
                    "geometry",
                    "organisation",
                    "reference",
                    "prefix",
                    "point",
                    "slug",
                ]
                + self.entity_fields
            }
            row["json"] = json.dumps(properties)
            if row["json"] == {}:
                del row["json"]

        # add geometry
        # defaulting point from the shape centroid should be in the pipeline
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
        """entity_row.

        :param  facts: Nested List of facts pertaining to an entity [[entity, field, value]] (and resource)
        :type   facts: list[list[str]]
        """
        # time ordered priority
        # TBD: handle primary versus secondary sources ..
        row = {}
        row["entity"] = facts[0][0]
        for fact in facts:
            row[fact[1]] = fact[2]
        return row

    def insert_entity(self, facts):
        row = self.entity_row(facts)
        row = self.migrate_entity(row)
        if row:
            self.insert("entity", self.entity_fields, row)

    def load_old_entities(self, path):
        """load the old-entity table"""

        fields = self.specification.schema["old-entity"]["fields"]
        entity_min = self.specification.schema[self.dataset].get("entity-minimum")
        entity_max = self.specification.schema[self.dataset].get("entity-maximum")
        if entity_min is None or entity_max is None:
            raise ValueError(
                "Entity minimum and Entity maximum are not defined in the specification for ",
                self.dataset,
            )
        entity_min = int(entity_min)
        entity_max = int(entity_max)
        logging.info(f"loading old-entity from {path}")
        self.connect()
        self.create_cursor()
        for row in csv.DictReader(open(path, newline="")):
            entity_id = int(row.get("old-entity"))
            if entity_min <= entity_id <= entity_max:
                self.insert("old-entity", fields, row)
        self.commit()
        self.disconnect()

    def load_entities(self):
        """load the entity table from the fact table"""
        self.connect()
        self.create_cursor()
        self.execute(
            "select entity, field, value from fact"
            "  where value != '' or field == 'end-date'"
            "  order by entity, field, entry_date"
        )
        results = self.cursor.fetchall()

        facts = []
        for fact in results:
            # If facts and fact does not point to same entity as first fact
            if facts and fact[0] != facts[0][0]:
                # Insert existing facts
                self.insert_entity(facts)
                # Reset facts list for new entity
                facts = []
            facts.append(fact)

        if facts:
            self.insert_entity(facts)

        self.commit()
        self.disconnect()

    def add_counts(self):
        """count the number of entities by resource"""
        self.connect()
        self.create_cursor()
        self.execute(
            "select resource, count(*)"
            "  from ("
            "    select distinct resource, fact.entity"
            "  from entity, fact, fact_resource"
            "  where entity.entity = fact.entity"
            "    and fact.fact = fact_resource.fact"
            "  ) group by resource"
        )
        results = self.cursor.fetchall()
        for result in results:
            resource = result[0]
            count = result[1]
            self.execute(
                f"update dataset_resource set entity_count = {count} where resource = '{resource}'"
            )
        self.commit()
        self.disconnect()

    def entry_date_upsert(self, table, fields, row, conflict_fields, update_fields):
        """
        Dataset specific upsert function that only replace values for more recent entry_dates.
        Will insert rows where no conflict is found. where there's a conflict it was compare entry dates
        and insert other field
        """
        self.execute(
            """
            INSERT INTO %s(%s)
            VALUES (%s)
            ON CONFLICT(%s) DO UPDATE SET %s
            WHERE excluded.entry_date>%s.entry_date
            ;
            """
            % (
                colname(table),
                ",".join([colname(field) for field in fields]),
                ",".join(["%s" % self.colvalue(row, field) for field in fields]),
                ",".join([colname(field) for field in conflict_fields]),
                ", ".join(
                    [
                        "%s=excluded.%s" % (colname(field), colname(field))
                        for field in update_fields
                    ]
                ),
                colname(table),
            )
        )

    def load_facts(self, path):
        logging.info(f"loading facts from {path}")

        fact_fields = self.specification.schema["fact"]["fields"]
        fact_resource_fields = self.specification.schema["fact-resource"]["fields"]
        fact_conflict_fields = ["fact"]
        fact_update_fields = [
            field for field in fact_fields if field not in fact_conflict_fields
        ]
        for row in csv.DictReader(open(path, newline="")):
            self.entry_date_upsert(
                "fact", fact_fields, row, fact_conflict_fields, fact_update_fields
            )
            self.insert("fact-resource", fact_resource_fields, row, upsert=True)

    def load_column_fields(self, path):

        fields = self.specification.schema["column-field"]["fields"]

        logging.info(f"loading column_fields from {path}")

        self.connect()
        self.create_cursor()
        for row in csv.DictReader(open(path, newline="")):
            row["resource"] = path.stem
            row["dataset"] = self.dataset
            self.insert("column-field", fields, row)

        self.commit()
        self.disconnect()

    def load_issues(self, path):
        self.connect()
        self.create_cursor()
        fields = self.specification.schema["issue"]["fields"]
        logging.info(f"loading issues from {path}")
        for row in csv.DictReader(open(path, newline="")):
            self.insert("issue", fields, row)

        self.commit()
        self.disconnect()

    def load_dataset_resource(self, path):
        fields = self.specification.schema["dataset-resource"]["fields"]

        logging.info(f"loading dataset-resource from {path}")

        self.connect()
        self.create_cursor()
        for row in csv.DictReader(open(path, newline="")):
            self.insert("dataset-resource", fields, row)

        self.commit()
        self.disconnect()

    def load_transformed(self, path):

        self.connect()
        self.create_cursor()
        self.load_facts(path)
        self.commit()
        self.disconnect()

    def load(self):
        pass
