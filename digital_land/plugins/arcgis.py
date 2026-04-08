import json
import logging
from esridump.dumper import EsriDumper

ALLOWED_ARCGIS_PARAMETERS = {"max_page_size"}


def get(collector, url, log={}, plugin="arcgis", parameters=None):
    try:
        logging.info("%s %s" % (plugin, url))
        content = None

        parameters = validate_parameters(parameters)

        dumper = EsriDumper(
            url,
            fields=None,
            **parameters,
        )

        response = dumper._request("GET", url)
        dumper.get_metadata()
        log["status"] = str(response.status_code)
        if parameters:
            log["parameters"] = parameters

        content = '{"type":"FeatureCollection","features":['
        sep = "\n"

        for feature in dumper:
            content += sep + json.dumps(feature)
            sep = ",\n"

        content += "]}"

        content = str.encode(content)

    except Exception as exception:
        logging.warning(exception)
        log["exception"] = type(exception).__name__

    return log, content


def validate_parameters(parameters):
    if parameters is None:
        return {}

    if not isinstance(parameters, dict):
        raise ValueError("ArcGIS parameters must be a dictionary")

    unknown = set(parameters) - ALLOWED_ARCGIS_PARAMETERS
    if unknown:
        raise ValueError(
            f"Unsupported ArcGIS parameters: {sorted(unknown)}. "
            f"Allowed parameters: {sorted(ALLOWED_ARCGIS_PARAMETERS)}"
        )

    if "max_page_size" in parameters:
        value = parameters["max_page_size"]
        if not isinstance(value, int) or value <= 0:
            raise ValueError("ArcGIS parameter 'max_page_size' must be a positive integer")

    return parameters
