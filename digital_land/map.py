class Mapper:

    """
    fix field names using the provide schema
    concatenate notes and other fields
    """

    def __init__(self, schema, typos={}):
        self.schema = schema
        self.typos = typos

    def headers(self, fieldnames):
        headers = {}

        for header in fieldnames:
            fieldname = self.schema.normalise(header)
            if fieldname in self.schema.fieldnames:
                headers[header] = fieldname
            if fieldname in self.schema.normalised:
                headers[header] = self.schema.normalised[fieldname]
            elif fieldname in self.typos:
                headers[header] = self.typos[fieldname]

        return headers

    def concatenate_fields(self, row, o):
        for fieldname, field in self.schema.fields.items():
            if "concatenate" in field.get("digital-land", {}):
                cat = field["digital-land"]["concatenate"]
                o.setdefault(fieldname, "")
                o[fieldname] = cat["sep"].join(
                    [o[fieldname]] + [row[h] for h in cat["fields"] if row.get(h, None)]
                )
        return o

    def map(self, reader):
        headers = self.headers(reader.fieldnames)

        for stream_data in reader:
            row = stream_data["row"]
            o = {}

            for header in headers:
                o[headers[header]] = row[header]

            o = self.concatenate_fields(row, o)

            yield {
                "resource": stream_data["resource"],
                "row": o,
            }
