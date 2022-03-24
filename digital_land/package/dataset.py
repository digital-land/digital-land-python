from collections import defaultdict
import csv
from decimal import Decimal
import json
import logging
import re

import shapely.wkt

from .sqlite import SqlitePackage

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
    "fact-resource": ["resource", "fact"],
    "column-field": ["dataset", "resource", "column", "field"],
    "issue": [
        "resource",
        "dataset",
        "line-number",
        "entry-number",
        "field",
        "issue-type",
    ],
    "dataset-resource": ["resource"],
}


class DatasetPackage(SqlitePackage):
    def __init__(self, dataset, organisation, organisation_resource_map, **kwargs):
        super().__init__(dataset, tables=tables, indexes=indexes, **kwargs)
        self.dataset = dataset
        self.entity_fields = self.specification.schema["entity"]["fields"]
        self.organisations = organisation.organisation
        self.organisation_resource_map = organisation_resource_map

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
                geometry = shapely.wkt.loads(wkt)
                geometry = geometry.centroid
                row["longitude"] = "%.6f" % round(Decimal(geometry.x), 6)
                row["latitude"] = "%.6f" % round(Decimal(geometry.y), 6)

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
        # hack until FactReference is reliable ..
        organisation_entity = self.organisations.get(
            row.get("organisation", ""), {}
        ).get("entity", "")
        entity_id = facts[0][0]

        row["entity"] = entity_id
        row["organisation-entity"] = organisation_entity
        fact_by_rank = defaultdict(list)
        for fact in facts:
            fact_name = fact[1]
            value = fact[2]
            resource = fact[3]
            fact_by_rank[fact_name].append({"value": value, "resource_hash": resource})
        for fact_name, fact_values in fact_by_rank.items():
            if len(fact_values) > 1:
                # We have more than once source here, prefer the primary
                primary_source_values = list(
                    filter(
                        lambda pair: pair["resource_hash"]
                        in self.organisation_resource_map[organisation_entity],
                        fact_values,
                    )
                )
                if len(primary_source_values) == 1:
                    # We have only one value for the primary source present, nice and easy
                    row[fact_name] = primary_source_values[0]["value"]
                elif len(primary_source_values) > 1:
                    # Check to see same resources
                    deduplicated_values = set(
                        val["value"] for val in primary_source_values
                    )
                    if len(deduplicated_values) > 1:
                        logger.warn(
                            f"Multiple primary source values for fact {fact_name} on {entity_id}: {deduplicated_values}"
                        )
                    else:
                        row[fact_name] = list(deduplicated_values)[0]
            else:
                row[fact_name] = fact_values[0]["value"]

        return row

    def insert_entity(self, facts):
        row = self.entity_row(facts)
        row = self.migrate_entity(row)
        if row:
            self.insert("entity", self.entity_fields, row)

    def load_entities(self):
        """load the entity table from the fact table"""
        self.connect()
        self.create_cursor()
        self.execute(
            "select entity, field, value, fact_resource.resource from fact"
            " join fact_resource on fact.fact = fact_resource.fact "
            "  where value != ''"
            "  order by entity, field, fact.entry_date, fact_resource.resource"
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

    def load_facts(self, path):
        logging.info(f"loading facts from {path}")

        fact_fields = self.specification.schema["fact"]["fields"]
        fact_resource_fields = self.specification.schema["fact-resource"]["fields"]

        for row in csv.DictReader(open(path, newline="")):
            self.insert("fact", fact_fields, row, upsert=True)
            self.insert("fact-resource", fact_resource_fields, row, upsert=True)

    def load_column_fields(self, path, resource):
        fields = self.specification.schema["column-field"]["fields"]

        logging.info(f"loading column_fields from {path}")

        for row in csv.DictReader(open(path, newline="")):
            row["resource"] = resource
            row["dataset"] = self.dataset
            self.insert("column-field", fields, row)

    def load_issues(self, path, resource):
        fields = self.specification.schema["issue"]["fields"]

        logging.info(f"loading issues from {path}")

        for row in csv.DictReader(open(path, newline="")):
            row["resource"] = resource
            row["dataset"] = self.dataset
            self.insert("issue", fields, row)

    def load_dataset_resource(self, path, resource):
        fields = self.specification.schema["dataset-resource"]["fields"]

        logging.info(f"loading dataset-resource from {path}")

        for row in csv.DictReader(open(path, newline="")):
            self.insert("dataset-resource", fields, row)

    def load_transformed(self, path):
        m = re.search(r"/([a-f0-9]+).csv$", path)
        resource = m.group(1)

        self.connect()
        self.create_cursor()
        self.load_facts(path)
        # self.load_issues(path.replace("transformed/", "issue/"), resource)
        self.load_column_fields(
            path.replace("transformed/", "var/column-field/"), resource
        )
        self.load_dataset_resource(
            path.replace("transformed/", "var/dataset-resource/"), resource
        )
        self.commit()
        self.disconnect()

    def load(self):
        pass
