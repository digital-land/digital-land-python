import io
import csv
import logging
from SPARQLWrapper import SPARQLWrapper, JSON


url_prefix = {
    "https://query.wikidata.org/sparql": "http://www.wikidata.org/entity/"
}


def sparql(endpoint_url, query, prefix=""):
    s = SPARQLWrapper(
        endpoint_url,
        agent="Mozilla/5.0 (Windows NT 5.1; rv:36.0) "
        "Gecko/20100101 Firefox/36.0",
    )

    s.setQuery(query)
    s.setReturnFormat(JSON)
    return s.query().convert()


def remove_prefix(value, prefix):
    if prefix and value.startswith(prefix):
        return value[len(prefix):]
    return value


def as_csv(data, prefix=""):
    fields = [field.replace("_", "-") for field in data["head"]["vars"]]

    content = io.StringIO()
    w = csv.DictWriter(content, sorted(fields), extrasaction="ignore")
    w.writeheader()

    for o in data["results"]["bindings"]:
        row = {}
        for field in fields:
            f = field.replace("-", "_")
            if f in o:
                value = o[f]["value"]
                row[field] = remove_prefix(value, prefix)

        w.writerow(row)

    return content.getvalue().encode()


def get(collector, url, log={}, plugin="sparql"):
    url, script = url.split("#")
    prefix = url_prefix.get(url, "")

    logging.info("%s %s %s %s" % (plugin, url, script, prefix))

    query = open(script).read()

    data = sparql(url, query)
    content = as_csv(data, prefix)

    return log, content
