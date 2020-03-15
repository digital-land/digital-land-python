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
"""


class Harmoniser:
    def __init__(self, schema, issues=None):
        self.schema = schema
        self.fieldnames = schema.current_fieldnames()
        self.issues = issues

        types.enum.init()
        types.organisation.init()

    def normalise_field(self, fieldname, value):
        if not value:
            return ""

        if self.issues:
            self.issues.fieldname = fieldname

        datatype = self.schema.field_type(fieldname)
        value = self.schema.strip(fieldname, value)
        return datatype.normalise(value, issues=self.issues)

    def check(self, o):
        for field in self.schema.required_fields:
            if not o.get(field, None):
                self.log_issue(field, "missing", "")

    def set_default(self, o, field, value):
        if value and not o[field]:
            self.log_issue(field, "default", value)
            o[field] = value
        return o

    def default(self, o):
        for field in self.default_fields:
            o = self.set_default(o, field, o[self.default_fields[field]])

        for field in self.default_values:
            o = self.set_default(o, field, self.default_values[field])

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
