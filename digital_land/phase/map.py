import re
from ..log import ColumnFieldLog
from .phase import Phase


normalise_pattern = re.compile(r"[^a-z0-9-_]")


def normalise(name):
    new_name = name.replace("_", "-")
    return re.sub(normalise_pattern, "", new_name.lower())


class MapPhase(Phase):
    """
    rename field names using the provided column map
    """

    def __init__(self, fieldnames, columns={}, log=None):
        self.columns = columns
        self.normalised_fieldnames = {normalise(f): f for f in fieldnames}
        if not log:
            log = ColumnFieldLog()
        self.log = log

    def log_headers(self, headers):
        for column, field in headers.items():
            self.log.add(column=column, field=field)

    def headers(self, fieldnames):
        headers = {}
        matched = []

        # loop to check if mapping exists and add it to header
        for header in sorted(fieldnames):
            fieldname = normalise(header)
            for pattern, value in self.columns.items():
                if fieldname == pattern:
                    matched.append(value)
                    headers[header] = value

        # check other remaining fields and ignore if header is same and has already been added
        for header in sorted(fieldnames):
            if header in headers:
                continue
            fieldname = normalise(header)
            if fieldname not in matched and fieldname in self.normalised_fieldnames:
                headers[header] = self.normalised_fieldnames[fieldname]
                continue

        # bit of a hack to ensure we take a coherent pair of coordinates
        if {"GeoX", "Easting"} <= headers.keys():
            item = headers.pop("GeoX")
            headers["GeoX"] = item

        if {"GeoY", "Northing"} <= headers.keys():
            item = headers.pop("GeoY")
            headers["GeoY"] = item

        return headers

    def process(self, stream):
        headers = None

        for block in stream:
            row = block["row"]

            if not headers:
                headers = self.headers(row.keys())
                self.log_headers(headers)

            o = {}

            for header in headers:
                if headers[header] == "IGNORE":
                    continue

                o[headers[header]] = row.get(header)

            block["row"] = o

            yield block
