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

# input_path = sys.argv[1]
# output_path = sys.argv[2]
# schema_path = sys.argv[3]

schema = json.load(open("schema/brownfield-land.json"))


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
        fieldnames = schema["digital-land"]["fields"]
        fields = {
            field["digital-land"]["field"]: field["name"]
            for field in schema["fields"]
            if "digital-land" in field and "field" in field["digital-land"]
        }
        for field in fieldnames:
            if not field in fields:
                fields[field] = field

        __import__("pdb").set_trace()
        resource = os.path.basename(os.path.splitext(input_path)[0])
        # reader = csv.DictReader(open(input_path, newline=""))

        with open(output_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                o = {}
                row["resource"] = resource

                # translate OrganisationURI into an organisation CURIE
                row["OrganisationURI"] = organisation.get(row["OrganisationURI"], "")

                for field in fieldnames:
                    o[field] = row[fields[field]]

                writer.writerow(o)
