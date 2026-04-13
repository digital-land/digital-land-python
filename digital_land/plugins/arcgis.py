import json
import logging
import time
from esridump.dumper import EsriDumper
from pydantic import ConfigDict, Field, ValidationError
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
    logging.info("%s %s" % (plugin, url))
    parameters = validate_parameters(parameters)

    retries = parameters.retries
    timeout = parameters.timeout
    retry_backoff_seconds = parameters.retry_backoff_seconds
    dumper_kwargs = {
        "fields": None,
        "max_page_size": parameters.max_page_size,
        "timeout": timeout,
        "pause_seconds": retry_backoff_seconds,
        "num_of_retry": max(retries, 0),
    }

    last_exception = None
    for attempt in range(1, retries + 2):
        step = "initialisation"
        try:
            dumper = EsriDumper(url, **dumper_kwargs)

            step = "initial-request"
            response = dumper._request("GET", url)
            log["status"] = str(response.status_code)

            step = "metadata"
            dumper.get_metadata()

            step = "feature-iteration"
            content = '{"type":"FeatureCollection","features":['
            sep = "\n"
            for feature in dumper:
                content += sep + json.dumps(feature)
                sep = ",\n"
            content += "]}"
            content = str.encode(content)
            return log, content
        except Exception as exception:
            content = None
            last_exception = exception
            log["exception"] = type(exception).__name__
            logging.warning(
                "ArcGIS fetch failed at step '%s' on attempt %s/%s for %s: %s",
                step,
                attempt,
                retries + 1,
                url,
                exception,
            )
            if attempt > retries:
                break
            sleep_seconds = retry_backoff_seconds * (2 ** (attempt - 1))
            time.sleep(sleep_seconds)

    if last_exception is not None:
        logging.warning(last_exception)
    return log, content


def validate_parameters(parameters):
    if parameters is None:
        parameters = {}

    if not isinstance(parameters, dict):
        raise ValueError("ArcGIS parameters must be a dictionary")

    try:
        return ArcGISParameters(**parameters)
    except ValidationError as exc:
        raise ValueError(f"Invalid ArcGIS parameters: {exc}") from exc
