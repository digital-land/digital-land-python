from datetime import datetime, date

from .register import Item, hash_value
from .collection import Collection


def get_failing_endpoints_from_registers(
    log_path, collection_directory, first_date, last_date=date.today()
):
    collection = Collection(collection_directory)
    collection.load()

    return get_failing_endpoints(
        collection.log.entries, collection.endpoint.entries, first_date, last_date
    )


def get_failing_endpoints(
    log_entries, endpoint_entries, first_date, last_date=date.today()
):
    active_endpoints = {
        endpoint["endpoint"]
        for endpoint in endpoint_entries
        if not endpoint["end-date"]
    }
    start_idx, end_idx = get_entries_between_keys(
        first_date,
        last_date,
        len(log_entries),
        lambda idx: datetime.fromisoformat(log_entries[idx]["entry-date"]).date(),
    )
    failing_endpoints = {}
    for idx in range(start_idx, end_idx + 1):
        log = log_entries[idx]
        endpoint = log["endpoint"]
        if endpoint in active_endpoints:
            collection_result, reason = has_collected_resource(log)
            if not collection_result:
                if endpoint not in failing_endpoints:
                    failing_endpoints[endpoint] = {
                        "url": log["endpoint-url"],
                        "reason": reason,
                        "failure_dates": [log["entry-date"]],
                    }
                else:
                    failing_endpoints[endpoint]["failure_dates"].append(
                        log["entry-date"]
                    )

    return failing_endpoints


def has_collected_resource(log_item):
    failure_reason = ""
    if "exception" in log_item:
        failure_reason = log_item["exception"]
        return False, failure_reason

    if int(log_item["status"]) != 200:
        failure_reason = "Status code: {}".format(log_item["status"])
        return False, failure_reason

    if "resource" not in log_item:
        failure_reason = "Resource not retrieved"
        return False, failure_reason

    return True, failure_reason


def add_new_source_endpoint(entry, collection_directory):

    collection = Collection(collection_directory)
    collection.load()

    entry["endpoint"] = hash_value(entry["endpoint-url"])

    add_new_endpoint(entry, collection.endpoint)
    add_new_source(entry, collection.source)

    collection.save_csv()


def start_date(entry):
    if entry["start-date"]:
        return datetime.strptime(entry["start-date"], "%Y-%m-%d").date()
    return ""


def end_date(entry):
    if "end-date" in entry:
        return datetime.strptime(entry["end-date"], "%Y-%m-%d").date()
    return ""


def entry_date(entry):
    return entry.get("entry-date", date.today().strftime("%Y-%m-%d"))


def add_new_endpoint(entry, endpoint_register):
    item = Item(
        {
            "endpoint": entry["endpoint"],
            "endpoint-url": entry["endpoint-url"],
            "entry-date": date.today().strftime("%Y-%m-%d"),
            "start-date": start_date(entry),
            "end-date": end_date(entry),
        }
    )
    endpoint_register.add_entry(item)


def add_new_source(entry, source_register):
    item = Item(
        {
            "collection": entry.get("collection", entry["pipeline"]),
            "pipeline": entry["pipeline"],
            "organisation": entry["organisation"],
            "endpoint": entry["endpoint"],
            "documentation-url": entry["documentation-url"],
            "licence": entry["licence"],
            "attribution": entry["attribution"],
            "entry-date": entry_date(entry),
            "start-date": start_date(entry),
            "end-date": end_date(entry),
        }
    )
    source_register.add_entry(item)


def get_entries_between_keys(start_key, end_key, length, register_lookup):
    if end_key < start_key:
        return None, None

    lo = 0
    hi = length
    return bisect_left(start_key, lo, hi, register_lookup), bisect_right(
        end_key, lo, hi, register_lookup
    )


def bisect_left(key, start, end, lookup):
    while start < end:
        mid = (start + end) // 2
        test_key = lookup(mid)
        if test_key < key:
            start = mid + 1
        else:
            end = mid

    return start


def bisect_right(key, start, end, lookup):
    while start < end:
        mid = (start + end) // 2
        test_key = lookup(mid)
        if key < test_key:
            end = mid
        else:
            start = mid + 1

    return start - 1
