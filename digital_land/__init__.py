import sys
import csv
import logging
from importlib.metadata import version, PackageNotFoundError

logger = logging.getLogger(__name__)
PACKAGE_NAME = "digital-land"
try:
    __version__ = version(PACKAGE_NAME)
except PackageNotFoundError:
    # package is not installed
    logger.error(
        f"Package '{PACKAGE_NAME}' is not installed. So can not retieve version."
    )
    pass


def csv_field_size_limit(field_size_limit=sys.maxsize):
    while True:
        try:
            csv.field_size_limit(field_size_limit)
            break
        except OverflowError:
            field_size_limit = int(field_size_limit / 10)


csv_field_size_limit()
