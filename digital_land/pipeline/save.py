import csv
import logging


def save(reader, path, fieldnames=None):

    logging.debug(f"saving {path}")

    with open(path, "w", newline="") as f:
        if not fieldnames:
            writer = csv.writer(f)
        else:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

        for row in reader:
            writer.writerow(row)
