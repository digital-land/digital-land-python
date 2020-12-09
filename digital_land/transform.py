#!/usr/bin/env python3

#
#  fix obvious issues in a brownfield land CSV
#  -- make it valid according to the 2019 guidance
#  -- log fixes as suggestions for the user to amend
#


class Transformer:
    def __init__(self, fields, transformations, collection=None):
        self.transformations = transformations
        self.fields = fields
        self.collection = collection

    def transform(self, reader):

        for stream_data in reader:
            row = stream_data["row"]
            o = {}
            row["resource"] = stream_data["resource"]

            if self.collection:
                orgs = self.collection.resource_organisations(row["resource"])
                if len(orgs) == 1:
                    row["OrganisationURI"] = orgs[0]

            for field in self.fields:
                if field in row and row[field]:
                    o[field] = row[field]
                elif (
                    field in self.transformations and self.transformations[field] in row
                ):
                    o[field] = row[self.transformations[field]]

            if set(["GeoX", "GeoY"]).issubset(row.keys()) and "point" in self.fields:
                o["point"] = f"POINT({row['GeoX']} {row['GeoY']})"

            yield {
                "resource": stream_data["resource"],
                "row": o,
            }
