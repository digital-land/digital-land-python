# CSV store, a set of CSV files of entries in a directory

import os
import csv
import logging
from pathlib import Path
from .memory import MemoryStore


class CSVStore(MemoryStore):
    def csv_path(store, directory=""):
        return Path(directory) / (store.schema.name + ".csv")

    def load_csv(self, path=None, directory=""):
        path = path or self.csv_path(directory)
        logging.debug("loading %s" % (path))
        reader = csv.DictReader(open(path, newline=""))
        for row in reader:
            self.add_entry(row)

    def load(self, *args, **kwargs):
        self.load_csv(*args, **kwargs)

    def save_csv(self, path=None, directory="", entries=None):
        path = path or self.csv_path(directory)

        if entries is None:
            entries = self.entries

        os.makedirs(os.path.dirname(path), exist_ok=True)
        logging.debug("saving %s" % (path))
        f = open(path, "w", newline="")
        writer = csv.DictWriter(
            f, fieldnames=self.schema.fieldnames, extrasaction="ignore"
        )
        writer.writeheader()
        for entry in entries:
            writer.writerow(entry)

    def save(self, *args, **kwargs):
        self.save_csv(*args, **kwargs)

    #  I think your right about validators, it should be a separate class that this one just calls this will help with making it a static method if
    # we need to
    @staticmethod
    def endpoint_url(endpoint_val: str = None) -> bool:
        """
        Checks whether the supplied parameter is valid according to
        business rules
        :param endpoint_val:
        :return: Boolean
        """
        if not endpoint_val:
            return False
        if type(endpoint_val) is float:
            return False

        return True

    @staticmethod
    def validate_organisation(org_name_val: str = None) -> bool:
        """
        Checks whether the supplied parameter is valid according to
        business rules
        :param org_name_val:
        :return: Boolean
        """
        if not org_name_val:
            return False
        if type(org_name_val) is float:
            return False

        return True

    @staticmethod
    def reference(ref_val: str = None) -> bool:
        """
        Checks whether the supplied parameter is valid according to
        business rules
        :return: Boolean
        """
        if not ref_val:
            return False
        if type(ref_val) is float:
            return False

        return True
