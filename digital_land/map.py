class Mapper:

    """
    fix field names using the provide schema
    concatenate notes and other fields
    """

    def __init__(self, schema):
        self.schema = schema
        self.typos = self.schema.typos()

    def headers(self, reader):
        headers = {}

        for header in reader.fieldnames:
            fieldname = self.schema.normalise(header)
            if fieldname in self.schema.fieldnames:
                headers[header] = fieldname
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

    def mapper(self, reader):
        headers = self.headers(reader)

        for row in reader:
            o = {}

            for header in headers:
                o[headers[header]] = row[header]

            o = self.concatenate_fields(row, o)

            yield o
