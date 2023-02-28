from datetime import datetime
import hashlib

from .register import Item, hash_value
from .collection import Collection


def recalculate_source_hashes(collection):
    for entry in collection.source.entries:
        key = "%s|%s|%s" % (
            entry["collection"],
            entry["organisation"],
            entry["endpoint"],
        )
        entry["source"] = hashlib.md5(key.encode()).hexdigest()


def add_source_endpoint(entry, directory=None, collection=None):
    if not collection:
        collection = Collection()
        collection.load()

    entry["endpoint"] = hash_value(entry["endpoint-url"])

    add_endpoint(entry, collection.endpoint)
    add_source(entry, collection.source)

    recalculate_source_hashes(collection)
    collection.save_csv()


def start_date(entry):
    if entry.get("start-date", ""):
        return datetime.strptime(entry["start-date"], "%Y-%m-%d").date()
    return ""


def end_date(entry):
    if entry.get("end-date", ""):
        return datetime.strptime(entry["end-date"], "%Y-%m-%d").date()
    return ""


def entry_date(entry):
    return entry.get("entry-date", datetime.utcnow().strftime("%Y-%m-%dT%H:%H:%M:%SZ"))


def add_endpoint(entry, endpoint_register):
    item = Item(
        {
            "endpoint": entry["endpoint"],
            "endpoint-url": entry["endpoint-url"],
            "plugin": entry.get("plugin", ""),
            "parameters": entry.get("parameters", ""),
            "entry-date": entry_date(entry),
            "start-date": start_date(entry),
            "end-date": end_date(entry),
        }
    )
    endpoint_register.add_entry(item)


def add_source(entry, source_register):
    item = Item(
        {
            "source": entry.get("source", ""),
            "collection": entry["collection"],
            "pipelines": entry.get("pipelines", entry["collection"]),
            "organisation": entry.get("organisation", ""),
            "endpoint": entry["endpoint"],
            "documentation-url": entry.get("documentation-url", ""),
            "licence": entry.get("licence", ""),
            "attribution": entry.get("attribution", ""),
            "entry-date": entry_date(entry),
            "start-date": start_date(entry),
            "end-date": end_date(entry),
        }
    )
    source_register.add_entry(item)
