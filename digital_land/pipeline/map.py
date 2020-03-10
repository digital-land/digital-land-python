#
#  fix field names using the provide schema
#

import os
import sys
import re
import csv
import json

input_path = sys.argv[1]
output_path = sys.argv[2]
schema_path = sys.argv[3]

schema = json.load(open(schema_path))
fields = {field["name"]: field for field in schema["fields"]}
fieldnames = [field["name"] for field in schema["fields"]]


def normalise(name):
    return re.sub(normalise.pattern, "", name.lower())


normalise.pattern = re.compile(r"[^a-z0-9]")


if __name__ == "__main__":
    # index of fieldname typos
    typos = {}
    for fieldname in fieldnames:
        field = fields[fieldname]
        typos[normalise(fieldname)] = fieldname
        if "title" in field:
            typos[normalise(field["title"])] = fieldname
        if "digital-land" in field:
            for typo in field["digital-land"].get("typos", []):
                typos[normalise(typo)] = fieldname

    reader = csv.DictReader(open(input_path, newline=""))

    # build index of headers from the input
    headers = {}
    if reader.fieldnames:
        for field in reader.fieldnames:
            fieldname = normalise(field)

            if fieldname not in fieldnames:
                if fieldname in typos:
                    headers[field] = typos[fieldname]

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            o = {}
            for header in headers:
                field = headers[header]
                o[field] = row[header]

            for fieldname, field in fields.items():
                if "concatenate" in field.get("digital-land", {}):
                    cat = field["digital-land"]["concatenate"]
                    o.setdefault(fieldname, "")
                    o[fieldname] = cat["sep"].join(
                        [o[fieldname]]
                        + [row[h] for h in cat["fields"] if row.get(h, None)]
                    )

            writer.writerow(o)
