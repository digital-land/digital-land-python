import sys
import csv
import logging
from importlib.metadata import version, PackageNotFoundError

logger = logging.getLogger(__name__)

try:
    __version__ = version("digital-land")
except PackageNotFoundError:
    # package is not installed
    logger.error("Package 'digital-land' is not installed. So can not retieve version.")
    pass


def csv_field_size_limit(field_size_limit=sys.maxsize):
    while True:
        try:
            csv.field_size_limit(field_size_limit)
            break
        except OverflowError:
            field_size_limit = int(field_size_limit / 10)


csv_field_size_limit()
