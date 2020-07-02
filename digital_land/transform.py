#!/usr/bin/env python3

#
#  fix obvious issues in a brownfield land CSV
#  -- make it valid according to the 2019 guidance
#  -- log fixes as suggestions for the user to amend
#
import os

# import sys
import csv
import json


class Transformer:
    def __init__(self, schema):
        self.schema = schema

    def transform(self, reader):

        # map of OrganisationURI to organisation CURIE
        organisation = {}
        for row in csv.DictReader(open("var/cache/organisation.csv", newline="")):
            if row.get("opendatacommunities", None):
                organisation[row["opendatacommunities"]] = row["organisation"]

        # map of harmonised fields to digital-land fields
        fieldnames = self.schema["digital-land"]["fields"]
        fields = {
            field["digital-land"]["field"]: field["name"]
            for field in self.schema["fields"]
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
            row["OrganisationURI"] = organisation.get(row["OrganisationURI"], "")

            for field in fieldnames:
                o[field] = row[fields[field]]

            yield {
                "resource": stream_data["resource"],
                "row": o,
            }
