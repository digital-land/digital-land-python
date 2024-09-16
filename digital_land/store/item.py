# a store of individual item.json files

import os
import glob
import logging
from datetime import datetime
from pathlib import Path
import pandas as pd

from ..register import Item
from .csv import CSVStore


class ItemStore(CSVStore):
    def item_path(self, item, directory=""):
        return Path(directory) / (item.hash + ".json")

    def save_item(self, item, path):
        data = item.pack()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if not os.path.exists(path):
            logging.info("saving %s" % (path))
            with open(path, "wb") as f:
                f.write(data)

    def save_items(self, path):
        raise NotImplementedError

    def save(self, *args, **kwargs):
        self.save_items(*args, **kwargs)

    def load_item(self, path):
        data = open(path).read()
        item = Item()
        item.unpack(data)
        return item

    def load_items(self, directory=".", after=None):
        path = "%s/*.json" % (directory)
        if after:
            after = datetime.fromisoformat(after)
            startdate = datetime(year=after.year, month=after.month, day=after.day)

        logging.debug("loading %s%s" % (path, f" after {after}" if after else ""))
        for path in sorted(glob.glob(path)):
            if after:
                # If we are loading items after a tinmestamp, we can skip anything
                # in a directory before the date part of that stamp. We want to INCLUDE
                # the directory of the 'after' date in case the item is later in the day.
                dirdate = datetime.fromisoformat(
                    os.path.split(os.path.dirname(path))[-1]
                )
                if dirdate < startdate:
                    continue

            item = self.load_item(path)
            if after and datetime.fromisoformat(item["entry-date"]) <= after:
                # Check the entry-date of the item against the one are want to load
                # after.
                continue

            self.add_entry(item)

    def load(self, *args, **kwargs):
        self.load_items(*args, **kwargs)


class CSVItemStore(CSVStore):
    def __init__(self, *args, **kwargs):
        self._latest_entry_date = None
        super().__init__(*args, **kwargs)

    def latest_entry_date(self):
        """Gets the latest entry date from the issue"""
        return self._latest_entry_date

    def load_items(self, directory=None, after=None):
        csv_files_path = os.path.join(directory, "*/*/*.csv")

        if after:
            after = datetime.fromisoformat(after)
            startdate = datetime(year=after.year, month=after.month, day=after.day)

            logging.debug(f"Loading files after {after}")

        logging.debug(f"Loading CSV files from {csv_files_path}")

        for csv_file_path in sorted(glob.glob(csv_files_path)):
            try:
                # Extract the date from the folder name (2 levels up from the file)
                dirdate_str = os.path.basename(
                    os.path.dirname(os.path.dirname(csv_file_path))
                )
                dirdate = datetime.fromisoformat(dirdate_str)

                # If the folder's date is before 'startdate', skip it
                if after and dirdate < startdate:
                    logging.debug(
                        f"Skipping file {csv_file_path} with dir date {dirdate_str}, before 'after' date"
                    )
                    continue

                # Read the CSV file into a DataFrame
                df = pd.read_csv(csv_file_path)

                # Fill missing 'entry-date' with folder date if applicable
                if "entry-date" not in df.columns:
                    df["entry-date"] = dirdate.strftime("%Y-%m-%d")
                else:
                    df["entry-date"] = df["entry-date"].fillna(
                        dirdate.strftime("%Y-%m-%d")
                    )

                # Convert each row in the DataFrame into a dictionary
                for _, row in df.iterrows():
                    item = row.to_dict()
                    # Skip items that have 'entry-date' earlier or equal to 'after'
                    if after and datetime.fromisoformat(item["entry-date"]) <= after:
                        logging.debug(
                            f"Skipping item with entry-date {item['entry-date']}"
                        )
                        continue
                    self.add_entry(item)  # Add the entry using the add_entry method

            except Exception as e:
                logging.error(f"Error loading {csv_file_path}: {e}")
                continue

    def add_entry(self, item):
        self.entries.append(item)
        # Update the _latest_entry_date
        if "entry-date" in item:
            if (
                not self._latest_entry_date
                or item["entry-date"] > self._latest_entry_date
            ):
                self._latest_entry_date = item["entry-date"]

    def load(self, *args, **kwargs):
        self.load_items(*args, **kwargs)
