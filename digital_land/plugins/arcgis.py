import json
import logging
from esridump.dumper import EsriDumper


def get(collector, url, log={}, plugin="arcgis"):
    try:
        logging.info("%s %s" % (plugin, url))
        content = None
        dumper = EsriDumper(url, fields=None)

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

    except Exception as exception:
        logging.warning(exception)
        log["exception"] = type(exception).__name__

    return log, content
