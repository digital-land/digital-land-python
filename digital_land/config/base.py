import logging 
import os
import csv

class ConfigBase:
    def file_reader(self, filename):
        # read a file from the pipeline path, ignore if missing
        path = os.path.join(self.path, filename)
        if not os.path.isfile(path):
            return []
        logging.debug(f"load {path}")
        return csv.DictReader(open(path))
