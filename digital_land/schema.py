import re
import json


class Schema:

    """
    frictionless data schema
    https://frictionlessdata.io/specs/table-schema/
    """

    normalise_re = re.compile(r"[^a-z0-9]")

    def __init__(self, path):
        self.schema = json.load(open(path))
        self.fields = {field["name"]: field for field in self.schema["fields"]}
        self.fieldnames = [field["name"] for field in self.schema["fields"]]

    def normalise(self, name):
        return re.sub(self.normalise_re, "", name.lower())

    def typos(self):
        typos = {}
        for fieldname in self.fieldnames:
            field = self.fields[fieldname]
            typos[self.normalise(fieldname)] = fieldname
            if "title" in field:
                typos[self.normalise(field["title"])] = fieldname
            if "digital-land" in field:
                for typo in field["digital-land"].get("typos", []):
                    typos[self.normalise(typo)] = fieldname
        return typos
