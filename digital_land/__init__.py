import os
import sys
import csv
import logging
from importlib.metadata import version, PackageNotFoundError

# On macOS, fork() after loading C extensions (numpy, pandas, pyproj, etc.)
# can segfault because the Objective-C runtime's post-fork safety checks abort
# the child process. Setting this before any C extension is imported ensures
# it is inherited by any subprocess spawned later (e.g. ogr2ogr via gdal_utils).
os.environ.setdefault("OBJC_DISABLE_INITIALIZE_FORK_SAFETY", "YES")

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
