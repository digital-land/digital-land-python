import re


class Mapper:

    """
    fix field names using the provided column map
    concatenate notes and other fields  # TODO reimplement concatenation!!
    """

    def __init__(self, fieldnames, column={}, concat={}):
        self.column = column
        self.concat = concat
        self.normalised_fieldnames = {self.normalise(f): f for f in fieldnames}

    def headers(self, fieldnames):
        headers = {}

        for header in fieldnames:
            fieldname = self.normalise(header)
                headers[header] = header
            if fieldname in self.normalised_fieldnames:
                headers[header] = self.normalised_fieldnames[fieldname]
                continue

            for pattern, value in self.column.items():
                if header == pattern:
                    headers[header] = value

        return headers

    def concatenate_fields(self, row, o):
        for fieldname, cat in self.concat.items():
            o[fieldname] = cat["separator"].join(
                filter(
                    None,
                    [o.get(fieldname, None)]
                    + [row.get(h, None) for h in cat["fields"]],
                )
            )
        return o

    #     for fieldname, field in self.schema.fields.items():
    #         if "concatenate" in field.get("digital-land", {}):
    #             cat = field["digital-land"]["concatenate"]
    #             o.setdefault(fieldname, "")
    #             o[fieldname] = cat["sep"].join(
    #                 [o[fieldname]] + [row[h] for h in cat["fields"] if row.get(h, None)]
    #             )
    #     return o

    normalise_pattern = re.compile(r"[^a-z0-9-]")

    def normalise(self, name):
        return re.sub(self.normalise_pattern, "", name.lower())

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
