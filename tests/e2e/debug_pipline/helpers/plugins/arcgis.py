import json
from esridump.dumper import EsriDumper


def get(collector, url, log={}, plugin="arcgis"):
    dumper = EsriDumper(url, fields=None)

    content = '{"type":"FeatureCollection","features":['
    sep = "\n"

    for feature in dumper:
        content += sep + json.dumps(feature)
        sep = ",\n"

    content += "]}"

    content = str.encode(content)
    return log, content
