import csv
import logging


def fsave(reader, f, fieldnames=None):
    if not fieldnames:
        writer = csv.writer(f)
        key = "line"
    else:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        key = "row"

    for row in reader:
        writer.writerow(row[key])


def save(reader, path, fieldnames=None):
    logging.debug(f"saving {path}")
    with open(path, "w", newline="") as f:
        fsave(reader, f, fieldnames=fieldnames)
