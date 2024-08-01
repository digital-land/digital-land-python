from datetime import datetime
from .phase import Phase
from pathlib import Path
from digital_land.datatype.point import PointDataType
from digital_land.datatype.factory import datatype_factory
import shapely.wkt
import logging
import warnings
import os
import csv

logger = logging.getLogger(__name__)

# Storing mandatory fields in dict for now until added to specification
MANDATORY_FIELDS_DICT = {
    "article-4-direction": [
        "reference",
        "name",
        "description",
        "document-url",
        "documentation-url",
    ],
    "article-4-direction-area": [
        "reference",
        "geometry",
        "name",
        "permitted-development-rights",
    ],
    "conservation-area": ["reference", "geometry", "name"],
    "conservation-area-document": [
        "reference",
        "name",
        "conservation-area",
        "document-url",
        "documentation-url",
        "document-type",
    ],
    "tree-preservation-order": [
        "reference",
        "name",
        "document-url",
        "documentation-url",
    ],
    "tree-preservation-zone": ["reference", "geometry"],
    "listed-building-outline": ["reference", "geometry", "name", "listed-building"],
    "tree": ["reference", "point", "geometry"],
}


class HarmonisePhase(Phase):
    def __init__(
        self, field_datatype_map=None, specification=None, issues=None, dataset=None
    ):
        self.field_datatype_map = field_datatype_map
        self.specification = specification
        self.issues = issues
        self.dataset = dataset

    def get_field_datatype_name(self, fieldname):
        if self.field_datatype_map:
            try:
                datatype_name = self.field_datatype_map[fieldname]
            except KeyError:
                raise ValueError(f"field {fieldname} does not have a datatype mapping")

        elif self.specification:
            warnings.warn(
                "providing specification is depreciated please provide field_datatype_map instead",
                DeprecationWarning,
                2,
            )
            datatype_name = self.specification.field[fieldname]["datatype"]

        else:
            raise ValueError("please provide field_datatype_map")

        return datatype_name

    def harmonise_field(self, fieldname, value):
        if not value:
            return ""

        self.issues.fieldname = fieldname
        datatype_name = self.get_field_datatype_name(fieldname)
        datatype = datatype_factory(datatype_name=datatype_name)
        return datatype.normalise(value, issues=self.issues)

    def process(self, stream):
        for block in stream:
            row = block["row"]
            self.issues.resource = block["resource"]
            self.issues.line_number = block["line-number"]
            self.issues.entry_number = block["entry-number"]

            o = {}

            for field in row:
                self.validate_categorical_fields(field, row[field])
                o[field] = self.harmonise_field(field, row[field])

            # remove future entry dates
            for field in ["entry-date", "LastUpdatedDate"]:
                if (
                    o.get(field, "")
                    and datetime.strptime(o[field][:10], "%Y-%m-%d").date()
                    > datetime.today().date()
                ):
                    self.issues.log_issue(
                        field,
                        "future entry-date",
                        row[field],
                        f"{field} must be today or in the past",
                    )
                    o[field] = ""

            # fix point geometry
            # TBD: generalise as a co-constraint
            if set(["GeoX", "GeoY"]).issubset(row.keys()):
                self.issues.fieldname = "GeoX,GeoY"

                point = PointDataType()
                try:
                    geometry = point.normalise(
                        [o["GeoX"], o["GeoY"]], issues=self.issues
                    )
                    point_geometry = shapely.wkt.loads(geometry)
                    x, y = point_geometry.coords[0]
                    (o["GeoX"], o["GeoY"]) = [str(x), str(y)]
                except Exception as e:
                    logger.error(
                        f"Exception occurred while fetching geoX, geoY coordinates: {e}"
                    )

            # ensure typology fields are a CURIE
            for typology in ["organisation", "geography", "document"]:
                value = o.get(typology, "")
                if value and ":" not in value:
                    o[typology] = "%s:%s" % (self.dataset, value)

            mandatory_fields = MANDATORY_FIELDS_DICT.get(self.dataset)

            # Check for missing values in mandatory fields
            # Only checking fields given to us - not checking for missing fields
            # One of geometry or point must not be empty if either field is given
            for field in row:
                if field in ["geometry", "point"]:
                    if (row.get("geometry") == "" or row.get("geometry") is None) and (
                        row.get("point") == "" or row.get("point") is None
                    ):
                        self.issues.log_issue(
                            field,
                            "missing value",
                            "",
                            f"{field} missing",
                        )
                elif mandatory_fields and field in mandatory_fields:
                    if row.get(field) == "" or row.get(field) is None:
                        self.issues.log_issue(
                            field,
                            "missing value",
                            "",
                            f"{field} missing",
                        )

            # migrate wikipedia URLs to a reference compatible with dbpedia CURIEs with a wikipedia-en prefix
            if row.get("wikipedia", "").startswith("http"):
                self.issues.log_issue(
                    "wikipedia", "removed URI prefix", row["wikipedia"]
                )
                o["wikipedia"] = row["wikipedia"].replace(
                    "https://en.wikipedia.org/wiki/", ""
                )
            block["row"] = o

            yield block

    def get_category_fields(self):
        category_fields = {}
        if self.specification:
            for _, row in self.specification.get_category_fields_query(
                self.dataset
            ).iterrows():
                category_fields.setdefault(row["dataset"], []).append(row["field"])
        return category_fields

    def get_valid_categories(self):
        valid_category_values = {}
        category_fields = self.get_category_fields()

        if self.dataset not in category_fields:
            print(
                f"Dataset {self.dataset} not found in category fields. Skipping valid categories CSV processing."
            )
            return valid_category_values

        csv_file = self._get_csv_file_path()
        print("Processing valid categories CSV FILE:", csv_file)

        if os.path.exists(csv_file):
            valid_category_values = self._read_csv_file(csv_file)
        else:
            print(f"CSV file {csv_file} does not exist.")

        return valid_category_values

    def _get_csv_file_path(self):
        base_path = Path(__file__).resolve().parents[5]
        return base_path / "var" / "cache" / f"{self.dataset}.csv"

    def _read_csv_file(self, csv_file):
        valid_category_values = {"reference": [], "name": []}
        with open(csv_file, mode="r") as file:
            for row in csv.DictReader(file):
                reference = row.get("reference")
                name = row.get("name")
                if reference:
                    valid_category_values["reference"].append(reference.lower())
                if name:
                    valid_category_values["name"].append(name.lower())
        return valid_category_values

    def validate_categorical_fields(self, fieldname, value):
        category_fields = self.get_category_fields()
        if fieldname in category_fields.get(self.dataset, []):
            valid_values = self.get_valid_categories()
            value_lower = value.lower()

            # Check against the name and reference lists
            if value_lower not in valid_values.get(
                "name"
            ) and value_lower not in valid_values.get("reference"):
                print("Issue was logged:", fieldname, value)
                self.issues.log_issue(fieldname, "invalid category values", value)
