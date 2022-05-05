from esridump.dumper import EsriDumper


def get(collector, url, log={}, plugin="arcgis"):
    dumper = EsriDumper(url)
    content = str.encode(str(list(dumper)))
    return log, content
