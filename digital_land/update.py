from datetime import datetime, date
from digital_land.register import Item
from digital_land.collection import LogRegister, EndpointRegister, SourceRegister
import hashlib


class ResourceEntry:

    fieldnames = [
        "attribution",
        "collection",
        "documentation_url",
        "endpoint_url",
        "endpoint",
        "licence",
        "organisation",
        "pipeline",
        "start_date",
        "end_date",
    ]

    def __init__(self, endpoint_url, organisation, **kwargs):
        self.__dict__.update((key, "") for key in self.fieldnames)
        self.endpoint_url = endpoint_url
        self.endpoint = hashlib.sha256(endpoint_url.encode("utf-8")).hexdigest()
        self.organisation = organisation
        self.__dict__.update((key,value) for key, value in kwargs.items() if key in self.fieldnames)

def get_failing_endpoints_from_registers(
    log_path, endpoints_path, first_date, last_date=date.today()
):
    log_register = LogRegister()
    endpoint_register = EndpointRegister()
    log_register.load_collection(log_path)
    endpoint_register.load(endpoints_path)
    return get_failing_endpoints(
        log_register.entries, endpoint_register.entries, first_date, last_date
    )


def get_failing_endpoints(
    log_entries, endpoint_entries, first_date, last_date=date.today()
):
    active_endpoints = {
        endpoint.item["endpoint"]
        for endpoint in endpoint_entries
        if not endpoint.item["end-date"]
    }
    start_idx, end_idx = get_entries_between_keys(
        first_date,
        last_date,
        len(log_entries),
        lambda idx: datetime.fromisoformat(log_entries[idx].item["entry-date"]).date(),
    )
    failing_endpoints = {}
    for idx in range(start_idx, end_idx + 1):
        log = log_entries[idx].item
        endpoint = log["endpoint"]
        if endpoint in active_endpoints:
            collection_result, reason = has_collected_resource(log)
            if not collection_result:
                if endpoint not in failing_endpoints:
                    failing_endpoints[endpoint] = {
                        "url": log["url"],
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


def add_new_resource_entry(resource_entry):
    endpoint_register = EndpointRegister("/Users/kishan.patelcommunities.gov.uk/Code/brownfield-land-pipeline/collection")
    source_register = SourceRegister("/Users/kishan.patelcommunities.gov.uk/Code/brownfield-land-pipeline/collection")
    endpoint_register.load()
    source_register.load()

    endpoint_entries = endpoint_register.entries.copy()
    endpoint_key = resource_entry.endpoint
    add_new_endpoint(endpoint_key, resource_entry.endpoint_url, endpoint_entries, endpoint_register.record)

    source_entries = source_register.entries.copy()
    add_new_source(resource_entry, source_entries, source_register.record)


def add_new_endpoint(endpoint_key, endpoint_url, endpoint_entries, records):
    if endpoint_key in records:
        existing_idx = records[endpoint_key][0]
        if endpoint_entries[existing_idx].item["end-date"]:
            print("WARNING: endpoint end-date {} found for URL {}".format(
                endpoint_entries[existing_idx].item["end-date"],
                endpoint_url))
        else:
            # No op if active entry already exists
            print("Active endpoint already exists for URL {}".format(
                endpoint_url))
            return

    endpoint_item = Item({"endpoint": endpoint_key,
                          "endpoint-url": endpoint_url,
                          "start-date": date.today().strftime("%Y-%m-%d"),
                          "end-date": ""})
    endpoint_entries.append(endpoint_item)


def add_new_source(resource_entry, source_entries, records):
    if resource_entry.endpoint in records:
        for idx in records[resource_entry.endpoint]:
            if resource_entry.organisation == source_entries[idx].item["organisation"]:
                if source_entries[idx].item["end-date"]:
                    print("WARNING: source end-date {} found for URL {}".format(
                        source_entries[idx].item["end-date"],
                        resource_entry.endpoint_url))
                    break
                else:
                    print("Active source entry already exists for organisation {} and URL {}".format(
                        resource_entry.organisation,
                        resource_entry.endpoint_url))
                    return

    source_item = Item({"collection": resource_entry.pipeline,
                        "pipeline": resource_entry.pipeline,
                        "organisation": resource_entry.organisation,
                        "endpoint": resource_entry.endpoint,
                        "documentation_url": resource_entry.documentation_url,
                        "licence": resource_entry.licence,
                        "attribution": resource_entry.attribution,
                        "start-date": date.today().strftime("%Y-%m-%d"),
                        "end-date": ""})
    source_entries.append(source_item)


def get_entries_between_keys(start_key, end_key, length, register_lookup):
    if end_key < start_key:
        return None, None

    lo = 0
    hi = length
    return bisect_left(start_key, lo, hi, register_lookup), bisect_right(end_key, lo, hi, register_lookup)


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

