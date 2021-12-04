#
#  normalise values by type defined in the schema
#  -- output is valid according to the 2019 guidance
#  -- log issues for suggestions for the user to amend
#

import re
from datetime import datetime

# import .digital_land.types as types
from digital_land.datatype.point import PointDataType


class Harmoniser:
    patch = {}

    def __init__(
        self,
        specification,
        pipeline,
        issues=None,
        collection={},
        organisation_uri=None,
        patch={},
        plugin_manager=None,
    ):
        self.specification = specification
        self.pipeline = pipeline
        self.default_values = {}
        self.default_fieldnames = {}
        # self.required_fieldnames = schema.required_fieldnames
        self.issues = issues
        self.collection = collection
        self.organisation_uri = organisation_uri
        self.patch = patch
        self.plugin_manager = plugin_manager

        if plugin_manager:
            plugin_manager.register(self)
            plugin_manager.hook.init_harmoniser_plugin(harmoniser=self)

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
            match = re.match(pattern, value, flags=re.IGNORECASE)
            if match:
                newvalue = match.expand(replacement)
                if newvalue != value:
                    self.log_issue(fieldname, "patch", value)
                return newvalue

        return value

    def set_default(self, o, fieldname, value):
        if fieldname not in o:
            return o
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
        self.default_values = {}
        if not resource:
            return

        self.default_fieldnames = self.pipeline.default_fieldnames(resource)
        resource_entry = self.collection.resource.records[resource][0]
        resource_organisations = self.collection.resource_organisations(resource)

        self.default_values["organisation"] = (
            resource_organisations[0] if len(resource_organisations) == 1 else ""
        )
        self.default_values["entry-date"] = resource_entry["start-date"]

        if self.plugin_manager:
            self.plugin_manager.hook.set_resource_defaults_post(resource=resource)

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

            for field in row:
                row[field] = self.apply_patch(field, row[field])
                if self.plugin_manager:
                    plugin_results = self.plugin_manager.hook.apply_patch_post(
                        fieldname=field, value=row[field]
                    )
                    if len(plugin_results) == 1:
                        row[field] = plugin_results[0]

                o[field] = self.harmonise_field(field, row[field])

            # default missing values
            o = self.default(o)

            # future entry dates
            for field in ["entry-date", "LastUpdatedDate"]:
                if (
                    o.get(field, "")
                    and datetime.strptime(o[field][:10], "%Y-%m-%d").date()
                    > datetime.today().date()
                ):
                    if self.issues:
                        self.issues.log("future %s" % field, row[field])
                    o[field] = ""

            # fix point geometry
            # TBD: generalise as a co-constraint
            if set(["GeoX", "GeoY"]).issubset(row.keys()):
                if self.issues:
                    self.issues.fieldname = "GeoX,GeoY"

                point = PointDataType()
                (o["GeoX"], o["GeoY"]) = point.normalise(
                    [o["GeoX"], o["GeoY"]], issues=self.issues
                )

            # ensure typology fields are a CURIE
            for typology in ["organisation", "geography", "document"]:
                value = o.get(typology, "")
                if value and ":" not in value:
                    o[typology] = "%s:%s" % (self.pipeline.name, value)

            last_resource = resource

            yield {
                "resource": resource,
                "row": o,
            }
