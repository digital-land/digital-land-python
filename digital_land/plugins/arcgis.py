import json
import logging
from esridump.dumper import EsriDumper
from pydantic import ConfigDict, Field
from pydantic.dataclasses import dataclass
from typing import Optional

DEFAULT_TIMEOUT = 60
DEFAULT_RETRIES = 2
DEFAULT_RETRY_BACKOFF_SECONDS = 2


@dataclass(config=ConfigDict(extra="forbid"))
class ArcGISParameters:
    max_page_size: Optional[int] = Field(default=None, gt=0)
    timeout: int = Field(default=DEFAULT_TIMEOUT, gt=0)
    retries: int = Field(default=DEFAULT_RETRIES, ge=0)
    retry_backoff_seconds: int = Field(
        default=DEFAULT_RETRY_BACKOFF_SECONDS,
        gt=0,
    )


def get(collector, url, log={}, plugin="arcgis", parameters=None):
    content = None
    logging.info("%s %s", plugin, url)

    if parameters is None:
        parameters = ArcGISParameters()
    elif not isinstance(parameters, ArcGISParameters):
        logging.warning(
            "ArcGIS get expects parameters to be an ArcGISParameters instance. using default parameters"
        )
        parameters = ArcGISParameters()

    retries = parameters.retries
    timeout = parameters.timeout
    retry_backoff_seconds = parameters.retry_backoff_seconds

    dumper_kwargs = {
        "fields": None,
        "max_page_size": parameters.max_page_size,
        "timeout": timeout,
        "pause_seconds": retry_backoff_seconds,
        "num_of_retry": retries,
    }

    try:
        dumper = EsriDumper(url, **dumper_kwargs)

        response = dumper._request("GET", url)
        dumper.get_metadata()
        response_status = str(response.status_code)

        content = '{"type":"FeatureCollection","features":['
        sep = "\n"

        for feature in dumper:
            content += sep + json.dumps(feature)
            sep = ",\n"

        content += "]}"
        content = str.encode(content)
        log["status"] = response_status

        log["status"] = response_status

    except Exception as exception:
        logging.warning(exception)
        log["exception"] = type(exception).__name__

    return log, content
