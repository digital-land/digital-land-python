from .phase import Phase


class TransformPhase(Phase):
    """
    change field names to match the latest specification
    """

    def __init__(self, fields, transformations, organisation={}):
        self.transformations = transformations
        self.fields = fields

        # map of OrganisationURI to organisation CURIE
        self.organisation_curie = {}
        for org in organisation.values():
            if org.get("opendatacommunities", None):
                self.organisation_curie[org["opendatacommunities"]] = org[
                    "organisation"
                ]

    def process(self, reader):
        for stream_data in reader:
            row = stream_data["row"]
            o = {}
            row["resource"] = stream_data["resource"]

            # translate OrganisationURI into an organisation CURIE
            if "OrganisationURI" in row:
                row["OrganisationURI"] = self.organisation_curie.get(
                    row["OrganisationURI"], ""
                )

            for field in self.fields:
                if field in row and row[field]:
                    o[field] = row[field]
                elif (
                    field in self.transformations and self.transformations[field] in row
                ):
                    o[field] = row[self.transformations[field]]

            if set(["GeoX", "GeoY"]).issubset(row.keys()) and "point" in self.fields:
                if row["GeoX"] and row["GeoY"]:
                    o["point"] = f"POINT({row['GeoX']} {row['GeoY']})"

            stream_data["row"] = o
            yield stream_data
