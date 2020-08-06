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
    patch_fields = set()

    def __init__(
        self,
        schema,
        specification,
        issues=None,
        resource_organisation=None,
        organisation_uri=None,
        patch={},
    ):
        self.schema = schema
        self.specification = specification
        # self.fieldnames = schema.fieldnames
        self.default_fieldnames = schema.default_fieldnames
        # self.required_fieldnames = schema.required_fieldnames
        self.issues = issues
        self.resource_organisation = resource_organisation
        self.organisation_uri = organisation_uri
        self.default_values = {}
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
        # value = self.schema.strip(fieldname, value)  # TODO what about strip?
        return datatype.normalise(value, issues=self.issues)

    def apply_patch(self, fieldname, value):
        for patch in self.patch[fieldname]:
            match = re.search(patch["expression"], value)
            if match:
                return match.expand(patch["value"])
        return value

    def check(self, o):
        pass
        # NO LONGER HAVE REQUIRED FIELDS... CONFIRM
        # 
        # for fieldname in self.required_fieldnames:
        #     if not o.get(fieldname, None):
        #         self.log_issue(fieldname, "missing", "")

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

    def set_resource_defaults(self, resource):
        if len(self.resource_organisation[resource]) > 0:
            resource_defaults = self.resource_organisation[resource]
            self.default_values["LastUpdatedDate"] = resource_defaults[0]["start-date"]
            if len(resource_defaults) > 1:
                # resource has more than one organisation
                self.default_values["OrganisationURI"] = ""
                return

            self.default_values["OrganisationURI"] = self.organisation_uri[
                resource_defaults[0]["organisation"].lower()
            ]

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
                self.set_resource_defaults(resource)

            o = {}

            for field in self.patch_fields:
                row[field] = self.apply_patch(field, row[field])

            for field in reader.fieldnames:
                o[field] = self.harmonise_field(field, row[field])

            # default missing values
            o = self.default(o)

            # check for missing required values
            self.check(o)

            # fix point geometry
            # TBD: generalise as a co-constraint
            if set(["GeoX", "GeoY"]).issubset(reader.fieldnames):
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
