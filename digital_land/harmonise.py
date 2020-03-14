#
#  normalise values by type defined in the schema
#  -- output is valid according to the 2019 guidance
#  -- log issues for suggestions for the user to amend
#

import os
import sys
import digital_land.types as types


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

        field = self.schema.fields[fieldname]
        constraints = field.get("constraints", {})
        extra = field.get("digital-land", {})

        for strip in extra.get("strip", []):
            value = re.sub(strip, "", value)
        value = value.strip()

        datatype = self.schema.field_type(field)

        return datatype.normalise(issues=self.issues)

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
            if set(["GeoX", "GeoY"]).issubset(fieldnames):
                if self.issues:
                    self.issues.fieldname = "GeoX,GeoY"
                (o["GeoX"], o["GeoY"]) = types.point.normalise(
                    [o["GeoX"], o["GeoY"]], issues=self.issues
                )

            yield o
