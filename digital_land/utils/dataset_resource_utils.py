import csv
import os


def read_dataset_resource_log(dataset_resource_dir, dataset, resource):
    """Read an existing DatasetResourceLog CSV for a given dataset/resource.

    Returns a dict with code-version, config-hash, and specification-hash,
    or None if the file doesn't exist or can't be read.

    Expected path: {dataset_resource_dir}/{dataset}/{resource}.csv
    """
    path = os.path.join(dataset_resource_dir, dataset, f"{resource}.csv")
    if not os.path.isfile(path):
        return None
    try:
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                return {
                    "code-version": row.get("code-version", ""),
                    "config-hash": row.get("config-hash", ""),
                    "specification-hash": row.get("specification-hash", ""),
                }
    except Exception:
        return None
    return None


def resource_needs_processing(
    dataset_resource_dir,
    dataset,
    resource,
    current_code_version,
    current_config_hash,
    current_specification_hash,
):
    """Check whether a resource needs processing by comparing its log against current state.

    Returns True if there is no existing log or if any of the three values differ.
    """
    log = read_dataset_resource_log(dataset_resource_dir, dataset, resource)
    if log is None:
        return True
    return (
        log["code-version"] != current_code_version
        or log["config-hash"] != current_config_hash
        or log["specification-hash"] != current_specification_hash
    )
