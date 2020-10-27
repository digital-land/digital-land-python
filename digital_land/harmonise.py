#
#  normalise values by type defined in the schema
#  -- output is valid according to the 2019 guidance
#  -- log issues for suggestions for the user to amend
#

# import .digital_land.types as types
from digital_land.datatype.point import PointDataType
import re

"""
# this could apply to any field, really
# TBD: collapse with organisation patches
def load_field_patches():
    for row in csv.DictReader(open("patch/enum.csv", newline="")):
        fieldname = row["field"]
        enum = row["enum"]
        value = normalise_value(row["value"])
        if enum not in field_enum[fieldname]:
            raise ValueError(
                "invalid '%s' enum '%s' in patch/enum.csv" % (fieldname, enum)
            )

# deduce default OrganisationURI and LastUpdatedDate default_values from path
# need to make this not file specific ..
def resource_organisation(default_values, input_path, resource_organisation_path):
    organisation = ""
    for row in csv.DictReader(open(resource_organisation_path), newline=""):
        if row["resource"] in input_path:
            default_values["LastUpdatedDate"] = row["start-date"]
            if not organisation:
                organisation = row["organisation"]
            elif organisation != row["organisation"]:
                # resource has more than one organisation
                default_values["OrganisationURI"] = ""
                return
    default_values["OrganisationURI"] = organisation_uri[organisation.lower()]
"""


class Harmoniser:
    patch = {}

    def __init__(
        self,
        specification,
        pipeline,
        issues=None,
        collection={},
        resource_organisation={},
        organisation_uri=None,
        patch={},
    ):
        self.specification = specification
        self.pipeline = pipeline
        self.default_values = {}
        self.default_fieldnames = {}
        # self.required_fieldnames = schema.required_fieldnames
        self.issues = issues
        self.collection = collection
        self.resource_organisation = resource_organisation
        self.organisation_uri = organisation_uri
        self.patch = patch

        # if "OrganisationURI" in self.fieldnames:
        #    if input_path and resource_organisation_path:
        #        resource_organisation(self.default_values, input_path, resource_organisation_path)

    def log_issue(self, field, issue, value):
        if self.issues:
            self.issues.log_issue(field, issue, value)

    def harmonise_field(self, fieldname, value):
        if not value:
            return ""

        if self.issues:
            self.issues.fieldname = fieldname

        datatype = self.specification.field_type(fieldname)
        return datatype.normalise(value, issues=self.issues)

    def apply_patch(self, fieldname, value):
        patches = {**self.patch.get(fieldname, {}), **self.patch.get("", {})}
        for pattern, replacement in patches.items():
            match = re.match(pattern, value.lower())
            if match:
                return match.expand(replacement)
        return value

    def set_default(self, o, fieldname, value):
        if value and not o[fieldname]:
            self.log_issue(fieldname, "default", value)
            o[fieldname] = value
        return o

    def default(self, o):
        for fieldname in self.default_fieldnames:
            for default_field in self.default_fieldnames[fieldname]:
                o = self.set_default(o, fieldname, o.get(default_field, ""))

        for fieldname in self.default_values:
            o = self.set_default(o, fieldname, self.default_values[fieldname])

        return o

    def set_resource_defaults_legacy(self, resource):
        # Used only for brownfield-land
        self.default_values = {}
        self.default_fieldnames = self.pipeline.default_fieldnames(resource)
        if len(self.resource_organisation.get(resource, [])) > 0:
            resource_defaults = self.resource_organisation[resource]
            self.default_values["LastUpdatedDate"] = resource_defaults[0]["start-date"]
            if len(resource_defaults) > 1:
                # resource has more than one organisation
                self.default_values["OrganisationURI"] = ""
                return

            self.default_values["OrganisationURI"] = self.organisation_uri[
                resource_defaults[0]["organisation"].lower()
            ]

    def set_resource_defaults(self, resource):
        self.default_values = {}
        if not resource:
            return
        self.default_fieldnames = self.pipeline.default_fieldnames(resource)
        resource_entry = self.collection.resource[resource][0].serialise()
        self.default_values["entry-date"] = resource_entry["start-date"]

        resource_organisations = self.collection.resource_organisation(resource)
        if len(resource_organisations) > 1:
            # resource has more than one organisation
            self.default_values["organisation"] = ""
            return

        self.default_values["organisation"] = resource_organisations[0]

    def harmonise(self, reader):

        if self.issues:
            self.issues.row_number = 0

        last_resource = None

        for stream_data in reader:
            row = stream_data["row"]
            resource = stream_data["resource"]

            if self.issues:
                self.issues.row_number += 1

            if not last_resource or last_resource != resource:
                if self.pipeline.name == "brownfield-land":
                    self.set_resource_defaults_legacy(resource)
                else:
                    self.set_resource_defaults(resource)

            o = {}

            for field in row:
                row[field] = self.apply_patch(field, row[field])
                o[field] = self.harmonise_field(field, row[field])

            # default missing values
            o = self.default(o)

            # fix point geometry
            # TBD: generalise as a co-constraint
            if set(["GeoX", "GeoY"]).issubset(row.keys()):
                if self.issues:
                    self.issues.fieldname = "GeoX,GeoY"

                point = PointDataType()
                (o["GeoX"], o["GeoY"]) = point.normalise(
                    [o["GeoX"], o["GeoY"]], issues=self.issues
                )

            last_resource = resource

            yield {
                "resource": resource,
                "row": o,
            }
