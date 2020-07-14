#!/usr/bin/env python3

#
#  fix obvious issues in a brownfield land CSV
#  -- make it valid according to the 2019 guidance
#  -- log fixes as suggestions for the user to amend
#


class Transformer:
    def __init__(self, schema, organisation):
        self.schema = schema

        # map of OrganisationURI to organisation CURIE
        self.organisation_curie = {}
        for org in organisation.values():
            if org.get("opendatacommunities", None):
                self.organisation_curie[org["opendatacommunities"]] = org[
                    "organisation"
                ]

    def transform(self, reader):

        # map of harmonised fields to digital-land fields
        fieldnames = self.schema.schema["digital-land"]["fields"]
        fields = {
            field["digital-land"]["field"]: field["name"]
            for field in self.schema.schema["fields"]
            if "digital-land" in field and "field" in field["digital-land"]
        }
        for field in fieldnames:
            if field not in fields:
                fields[field] = field

        for stream_data in reader:
            row = stream_data["row"]
            o = {}
            row["resource"] = stream_data["resource"]

            # translate OrganisationURI into an organisation CURIE
            row["OrganisationURI"] = self.organisation_curie.get(
                row["OrganisationURI"], ""
            )

            for field in fieldnames:
                o[field] = row[fields[field]]

            yield {
                "resource": stream_data["resource"],
                "row": o,
            }
