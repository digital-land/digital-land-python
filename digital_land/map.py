class Mapper:

    """
    fix field names using the provided column map
    concatenate notes and other fields  # TODO reimplement concatenation!!
    """

    def __init__(self, fieldnames, column):
        self.column = column
        self.output_fieldnames = fieldnames

    def headers(self, fieldnames):
        headers = {}

        for header in fieldnames:
            if header in self.output_fieldnames:
                headers[header] = header
                continue

            for pattern, value in self.column.items():
                if header == pattern:
                    headers[header] = value

        return headers

    # def concatenate_fields(self, row, o):
    #     for fieldname, field in self.schema.fields.items():
    #         if "concatenate" in field.get("digital-land", {}):
    #             cat = field["digital-land"]["concatenate"]
    #             o.setdefault(fieldname, "")
    #             o[fieldname] = cat["sep"].join(
    #                 [o[fieldname]] + [row[h] for h in cat["fields"] if row.get(h, None)]
    #             )
    #     return o

    def map(self, reader):
        headers = self.headers(reader.fieldnames)

        for stream_data in reader:
            row = stream_data["row"]
            o = {}

            for header in headers:
                o[headers[header]] = row[header]

            # o = self.concatenate_fields(row, o)

            yield {
                "resource": stream_data["resource"],
                "row": o,
            }
