#
#  normalise values by type defined in the schema
#  -- output is valid according to the 2019 guidance
#  -- log issues for suggestions for the user to amend
#

import digital_land.types as types

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
    def __init__(
        self, schema, issues=None, input_path=None, resource_organisation_path=None
    ):
        self.schema = schema
        self.fieldnames = schema.current_fieldnames()
        self.default_fieldnames = schema.default_fieldnames()
        self.required_fieldnames = schema.required_fieldnames()
        self.issues = issues
        self.default_values = {}

        # if "OrganisationURI" in self.fieldnames:
        #    if input_path and resource_organisation_path:
        #        resource_organisation(self.default_values, input_path, resource_organisation_path)

    def normalise_field(self, fieldname, value):
        if not value:
            return ""

        if self.issues:
            self.issues.fieldname = fieldname

        datatype = self.schema.field_type(fieldname)
        value = self.schema.strip(fieldname, value)
        return datatype.normalise(value, issues=self.issues)

    def check(self, o):
        for fieldname in self.required_fieldnames:
            if not o.get(fieldname, None):
                self.log_issue(fieldname, "missing", "")

    def set_default(self, o, fieldname, value):
        if value and not o[fieldname]:
            self.log_issue(fieldname, "default", value)
            o[fieldname] = value
        return o

    def default(self, o):
        for fieldname in self.default_fieldnames:
            o = self.set_default(o, fieldname, o[self.default_fieldnames[fieldname]])

        for fieldname in self.default_values:
            o = self.set_default(o, fieldname, self.default_values[fieldname])

        return o

    def harmoniser(self, reader):

        if self.issues:
            self.issues.row_number = 0

        for row in reader:
            if self.issues:
                self.issues.row_number += 1

            o = {}
            for field in self.fieldnames:
                o[field] = self.normalise_field(field, row[field])

            # default missing values
            o = self.default(o)

            # check for missing required values
            self.check(o)

            # fix point geometry
            # TBD: generalise as a co-constraint
            if set(["GeoX", "GeoY"]).issubset(self.fieldnames):
                if self.issues:
                    self.issues.fieldname = "GeoX,GeoY"

                (o["GeoX"], o["GeoY"]) = types.point.normalise(
                    [o["GeoX"], o["GeoY"]], issues=self.issues
                )

            yield o
