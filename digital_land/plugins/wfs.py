import io
import logging
import os
import re
import subprocess
import tempfile
from typing import Optional

from digital_land.phase.convert import detect_encoding
from pydantic import ConfigDict, Field
from pydantic.dataclasses import dataclass


# TBD: split this code into a WFS API plugin and a canonicalisation step

DEFAULT_PAGE_SIZE = 1000


strip_exps = [
    (re.compile(rb' ?timeStamp="[^"]*"'), rb""),
    (re.compile(rb' ?fid="[^"]*"'), rb""),
    (re.compile(rb'(gml:id="[^."]+)[^"]*'), rb"\1"),
]


@dataclass(config=ConfigDict(extra="forbid"))
class WFSParameters:
    paging: bool = False
    page_size: int = Field(default=DEFAULT_PAGE_SIZE, gt=0)
    source_url: Optional[str] = None
    layer_name: Optional[str] = None


@dataclass
class WFSFileResource:
    path: str
    cleanup: bool = True


def strip_variable_content(content):
    for strip_exp, replacement in strip_exps:
        content = strip_exp.sub(replacement, content)
    return content


def get(collector, url, log={}, plugin="wfs", parameters=None):
    if parameters is None:
        parameters = WFSParameters()
    elif not isinstance(parameters, WFSParameters):
        logging.warning(
            "WFS get expects parameters to be a WFSParameters instance. using default parameters"
        )
        parameters = WFSParameters()

    if parameters.paging:
        return get_paged_wfs(url, log, plugin=plugin, parameters=parameters)

    log, content = collector.get(url=url, log=log, plugin=plugin)
    encoding = detect_encoding(io.BytesIO(content))
    if encoding:
        content = strip_variable_content(content)
    return log, content


def get_paged_wfs(url, log, plugin="wfs", parameters=None):
    parameters = parameters or WFSParameters()
    output_file = tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False)
    output_path = output_file.name
    output_file.close()
    _remove_file(output_path)

    source = parameters.source_url or url
    command = [
        "ogr2ogr",
        "--config",
        "OGR_WFS_PAGING_ALLOWED",
        "ON",
        "--config",
        "OGR_WFS_PAGING_PAGE_SIZE",
        str(parameters.page_size),
        "-f",
        "GPKG",
        output_path,
        f"WFS:{source}",
    ]
    if parameters.layer_name:
        command.append(parameters.layer_name)

    logging.info("%s %s", plugin, url)

    try:
        result = subprocess.run(
            command,
            capture_output=True,
            check=False,
        )
    except Exception as exception:
        logging.warning(exception)
        log["exception"] = type(exception).__name__
        _remove_file(output_path)
        return log, None

    log["status"] = "200" if result.returncode == 0 else str(result.returncode)

    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        logging.warning("ogr2ogr failed (%s): %s", result.returncode, stderr)
        log["exception"] = "CalledProcessError"
        _remove_file(output_path)
        return log, None

    if not os.path.getsize(output_path):
        log["exception"] = "EmptyWFSResponse"
        _remove_file(output_path)
        return log, None

    return log, WFSFileResource(output_path)


def _remove_file(path):
    try:
        os.unlink(path)
    except OSError:
        pass
