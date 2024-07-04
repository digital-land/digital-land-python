import re
import subprocess
from packaging.version import Version


def get_gdal_version():
    out, _ = subprocess.Popen(
        ["ogr2ogr", "--version"],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    ).communicate()

    return Version(re.compile(r"GDAL\s([0-9.]+),").match(out.decode("ascii")).group(1))
