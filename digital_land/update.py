from datetime import datetime, date
from digital_land.collection import LogRegister, EndpointRegister

def get_failing_endpoints_from_registers(log_path, endpoints_path, first_date, last_date=date.today()):
    log_register = LogRegister()
    endpoint_register = EndpointRegister()
    log_register.load_collection(log_path)
    endpoint_register.load(endpoints_path)
    return get_failing_endpoints(log_register.entries, endpoint_register.entries, first_date, last_date)


def get_failing_endpoints(log_entries, endpoint_entries, first_date, last_date=date.today()):
    active_endpoints = \
        {endpoint.item['endpoint'] for endpoint in endpoint_entries if not endpoint.item['end-date']}
    start_idx, end_idx = get_entries_between_keys(first_date, last_date, len(log_entries),
                                                  lambda idx: datetime.fromisoformat(
                                                      log_entries[idx].item['entry-date']).date())
    failing_endpoints = {}
    for idx in range(start_idx, end_idx+1):
        log = log_entries[idx].item
        endpoint = log['endpoint']
        if endpoint in active_endpoints:
            collection_result, reason = has_collected_resource(log)
            if not collection_result:
                if endpoint not in failing_endpoints:
                    failing_endpoints[endpoint] = {
                        "url": log['url'],
                        "reason": reason,
                        "failure_dates": [log['entry-date']]
                    }
                else:
                    failing_endpoints[endpoint]["failure_dates"].append(log['entry-date'])

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

def get_entries_between_keys(start_key, end_key, length, register_lookup):
    if end_key < start_key:
        return -1, -1

    lo = 0
    hi = length

    #Binary search adapted from bisect module. See bisect_left and bisect_right.
    while lo < hi:
        mid = (lo + hi) // 2
        key = register_lookup(mid)
        if key < start_key:
            lo = mid + 1
        else:
            hi = mid

    start_idx = lo
    lo = 0
    hi = length

    while lo < hi:
        mid = (lo + hi) // 2
        key = register_lookup(mid)
        if end_key < key:
            hi = mid
        else:
            lo = mid + 1

    last_idx = lo - 1
    return start_idx, last_idx
