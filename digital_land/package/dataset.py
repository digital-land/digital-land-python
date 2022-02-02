import csv
import json
import re
import geojson
import shapely.wkt
from decimal import Decimal
import logging
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
    "issue": [
        "resource",
        "dataset",
        "line-number",
        "entry-number",
        "field",
        "issue-type",
    ],
}


def curie(value):
    s = value.split(":", 2)
    if len(s) == 2:
        return s
    return ["", value]


class DatasetPackage(SqlitePackage):
    def __init__(self, dataset, **kwargs):
        super().__init__(dataset, tables=tables, indexes=indexes, **kwargs)
        self.dataset = dataset
        self.entity_fields = self.specification.schema["entity"]["fields"]
        self.spatialite()

    def migrate_entity(self, row):
        dataset = self.dataset
        row["dataset"] = dataset
        entity = row["entity"]
        key_field = self.specification.schema[dataset]["key-field"]
        key_value = row.get(key_field, "")

        if not row.get("typology", ""):
            typology = self.specification.field[key_field]["typology"]
            row["typology"] = typology

        if not row.get("name", ""):
            row["name"] = ""

        # default the CURIE
        prefix = row.get("prefix", "")
        reference = row.get("reference", "")
        typology_value = row.get(typology, "")

        reference_prefix, reference_reference = curie(reference)
        typology_prefix, typology_reference = curie(typology_value)
        key_prefix, key_reference = curie(key_value)
        spec_prefix = self.specification.schema[dataset].get("prefix", "")

        row["prefix"] = (
            prefix
            or reference_prefix
            or typology_prefix
            or key_prefix
            or spec_prefix
            or dataset
        )
        row["reference"] = reference_reference or typology_reference or key_reference

        if not row["reference"]:
            logging.error(f"{dataset} entity {entity}: missing reference")

        if not row.get("name", ""):
            row["name"] = ""

        # migrate wikipedia URLs to a reference compatible with dbpedia CURIEs with a wikipedia-en prefix
        if row.get("wikipedia", ""):
            row["wikipedia"] = row["wikipedia"].replace(
                "https://en.wikipedia.org/wiki/", ""
            )

        # add other fields as JSON properties
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
                    typology,
                    dataset,
                    "point",
                    "slug",
                ]
                + self.entity_fields
            }
            row["json"] = json.dumps(properties)
            if row["json"] == {}:
                del row["json"]

        # add geometry
        # defaulting point from the shape centroid should probaby be in the pipeline
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
        self.insert("entity", self.entity_fields, row)

    def load_entities(self):
        """load the entity table from the fact table"""
        self.connect()
        self.create_cursor()
        self.execute(
            "select entity, field, value from fact"
            "  where value != ''"
            "  order by entity, field, entry_date"
        )
        results = self.cursor.fetchall()

        facts = []
        for fact in results:
            if facts and fact[0] != facts[0][0]:
                self.insert_entity(facts)
                facts = []
            facts.append(fact)

        if facts:
            self.insert_entity(facts)

        self.commit()
        self.disconnect()

    def load_facts(self, path):
        logging.info(f"loading facts from {path}")

        fact_fields = self.specification.schema["fact"]["fields"]
        fact_resource_fields = self.specification.schema["fact-resource"]["fields"]

        for row in csv.DictReader(open(path, newline="")):
            self.insert("fact", fact_fields, row, upsert=True)
            self.insert("fact-resource", fact_resource_fields, row, upsert=True)

    def load_issues(self, path, resource):
        fields = self.specification.schema["issue"]["fields"]

        logging.info(f"loading issues from {path}")

        for row in csv.DictReader(open(path, newline="")):
            row["resource"] = resource
            row["pipeline"] = self.dataset
            self.insert("issue", fields, row)

    def load_transformed(self, path):
        m = re.search(r"/([a-f0-9]+).csv$", path)
        resource = m.group(1)

        self.connect()
        self.create_cursor()
        self.load_facts(path)
        self.load_issues(path.replace("transformed/", "issue/"), resource)
        self.commit()
        self.disconnect()

    def load(self):
        pass
