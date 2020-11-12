import re
import itertools


class Mapper:

    """
    fix field names using the provided column map
    concatenate notes and other fields
    """

    def __init__(self, fieldnames, column={}, concat={}):
        self.column = column
        self.concat = concat
        self.normalised_fieldnames = {self.normalise(f): f for f in fieldnames}

    def headers(self, fieldnames):
        headers = {}
        matched = []
        for header in fieldnames:
            fieldname = self.normalise(header)

            for pattern, value in self.column.items():
                if value in matched:
                    # avoid clashing with previously mapped column
                    continue
                if fieldname == pattern:
                    matched.append(value)
                    headers[header] = value

            # stop if we found a match
            if header in headers:
                continue

            if fieldname in self.normalised_fieldnames:
                headers[header] = self.normalised_fieldnames[fieldname]
                continue

        return headers

    def concatenate_fields(self, row, o):
        for fieldname, cat in self.concat.items():
            o[fieldname] = cat["separator"].join(
                filter(
                    None,
                    itertools.chain(
                        [o.get(fieldname, None)],
                        [
                            row[h]
                            for h in cat["fields"]
                            if h in row and row[h].strip() != ""
                        ],
                    ),
                )
            )
        return o

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
