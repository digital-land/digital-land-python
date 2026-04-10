import json
import logging
import time
from esridump.dumper import EsriDumper
from utils.validate_parameter_utils import non_negative_int
from utils.validate_parameter_utils import positive_int
from utils.validate_parameter_utils import validate_plugin_parameters

DEFAULT_TIMEOUT = 60
DEFAULT_RETRIES = 2
DEFAULT_RETRY_BACKOFF_SECONDS = 2
ARCGIS_PARAMETER_DEFAULTS = {
    "timeout": DEFAULT_TIMEOUT,
    "retries": DEFAULT_RETRIES,
    "retry_backoff_seconds": DEFAULT_RETRY_BACKOFF_SECONDS,
}
ARCGIS_PARAMETER_VALIDATORS = {
    "max_page_size": positive_int,
    "timeout": positive_int,
    "retries": non_negative_int,
    "retry_backoff_seconds": positive_int,
}


def get(collector, url, log={}, plugin="arcgis", parameters=None):
    content = None
    logging.info("%s %s" % (plugin, url))
    parameters = validate_parameters(parameters)
    log["arcgis-parameters"] = parameters.copy()

    retries = parameters["retries"]
    timeout = parameters["timeout"]
    retry_backoff_seconds = parameters["retry_backoff_seconds"]
    dumper_kwargs = {
        "fields": None,
        "max_page_size": parameters.get("max_page_size"),
        "timeout": timeout,
        "pause_seconds": retry_backoff_seconds,
        "num_of_retry": max(retries, 0),
    }

    last_exception = None
    last_step = None
    for attempt in range(1, retries + 2):
        log["arcgis-attempt"] = attempt
        try:
            dumper = EsriDumper(url, **dumper_kwargs)

            log["arcgis-step"] = "initial-request"
            response = dumper._request("GET", url)
            log["status"] = str(response.status_code)

            log["arcgis-step"] = "metadata"
            dumper.get_metadata()

            log["arcgis-step"] = "feature-iteration"
            content = '{"type":"FeatureCollection","features":['
            sep = "\n"

            for feature in dumper:
                content += sep + json.dumps(feature)
                sep = ",\n"

            content += "]}"
            content = str.encode(content)
            if attempt > 1:
                log["arcgis-retried"] = attempt - 1
            log.pop("exception", None)
            log.pop("arcgis-next-retry-delay-seconds", None)
            return log, content
        except Exception as exception:
            content = None
            last_exception = exception
            last_step = log.get("arcgis-step", "initialisation")
            log["exception"] = type(exception).__name__
            log["arcgis-failed-step"] = last_step
            log["arcgis-timeout-seconds"] = timeout
            log["arcgis-retries"] = retries
            log["arcgis-retry-backoff-seconds"] = retry_backoff_seconds
            logging.warning(
                "ArcGIS fetch failed at step '%s' on attempt %s/%s for %s: %s",
                last_step,
                attempt,
                retries + 1,
                url,
                exception,
            )
            if attempt > retries:
                break
            sleep_seconds = retry_backoff_seconds * (2 ** (attempt - 1))
            log["arcgis-next-retry-delay-seconds"] = sleep_seconds
            time.sleep(sleep_seconds)

    if last_exception is not None:
        logging.warning(last_exception)
    return log, content


def validate_parameters(parameters):
    return validate_plugin_parameters(
        parameters=parameters,
        plugin_name="ArcGIS",
        defaults=ARCGIS_PARAMETER_DEFAULTS,
        validators=ARCGIS_PARAMETER_VALIDATORS,
    )
