import re

from .base import ConfigBase

class LookUps(ConfigBase):
    """
    A model for controlling the lookups associated with a collection, should be used to interact with the look up csv
    """
    def __init__(self,path):
        self.path = path
        self.lookups = self.load_lookups()

    def load_lookups(self):
        for row in self.file_reader(self.path):

            # migrate old lookup.csv files
            entry_number = row.get("entry-number", "")
            prefix = (
                row.get("prefix", "")
                or row.get("dataset", "")
                or row.get("pipeline", "")
            )
            reference = row.get("reference", "") or row.get("value", "")

            # composite key, ordered by specificity
            resource_lookup = self.lookups.setdefault(row.get("resource", ""), {})
            resource_lookup[
                self.key(
                    entry_number=entry_number,
                    prefix=prefix,
                    reference=reference,
                )
            ] = row["entity"]

            organisation = row.get("organisation", "")
            resource_lookup[
                self.key(
                    prefix=prefix,
                    reference=reference,
                    organisation=organisation,
                )
            ] = row["entity"]

    def normalise(value):
        normalise_pattern = re.compile(r"[^a-z0-9-]")
        return re.sub(normalise_pattern, "", value.lower())
    
    def key(self,entry_number="", prefix="", reference="", organisation=""):
        entry_number = str(entry_number)
        prefix = self.normalise(prefix)
        reference = self.normalise(reference)
        organisation = self.normalise(organisation)
        return ",".join([entry_number, prefix, reference, organisation])

    def lookup(self,**kwargs):
        self.lookups.get(self.key(**kwargs), "")