import re
import subprocess
from functools import lru_cache
from packaging.version import Version


@lru_cache(maxsize=None)
def get_gdal_version():
    out, _ = subprocess.Popen(
        ["ogr2ogr", "--version"],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    ).communicate()

    return Version(re.compile(r"GDAL\s([0-9.]+)").match(out.decode("ascii")).group(1))
