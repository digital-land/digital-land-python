import re


class Slugger:
    def __init__(self, prefix, key_field):
        self.prefix = prefix
        self.key_field = key_field

    def _generate_slug(self, row):
        return self.generate_slug(self.prefix, self.key_field, row)

    bad_chars = re.compile(r'[^A-Za-z0-9]')

    @staticmethod
    def generate_slug(prefix, key_field, row):
        """
        Helper method to provide slug without a Slugger instance
        """
        for field in ["organisation", key_field]:
            if field not in row or not row[field]:
                return None

        key = Slugger.bad_chars.sub("-", row[key_field])

        return f"{prefix}/{row['organisation'].replace(':','/')}/{key}"

    def slug(self, reader):
        for stream_data in reader:
            row = stream_data["row"]
            row["slug"] = self._generate_slug(row)
            yield {"resource": stream_data["resource"], "row": row}
