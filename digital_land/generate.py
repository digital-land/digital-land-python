from collections import defaultdict
import csv
import json
import sys


def load_csv(file):
    with open(file) as f:
        yield from csv.DictReader(f)


def json_schema(schema_path):
    fields = load_csv(schema_path + "field.csv")

    enums = defaultdict(list)
    for enum in load_csv(schema_path + "enum.csv"):
        enums[enum["field"]].append(enum["value"])

    output_fields = []
    for field in fields:
        if field["datatype"] == "uri":
            type_ = "string"
            format_ = "uri"
        else:
            type_ = field["datatype"]
            format_ = None

        new_row = {
            "name": field["field"],
            "title": field["name"],
            "description": field["description"],
            "type": type_,
        }

        if format_:
            new_row["format"] = format_

        if field["field"] in enums:
            new_row["constraints"] = {"enum": enums[field["field"]]}

        output_fields.append(new_row)

    json.dump({"fields": output_fields}, sys.stdout)
