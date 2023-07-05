import csv
import os
from pathlib import Path

from digital_land.collect import Collector, FetchStatus
from digital_land.collection import Collection


def get_endpoints_info(collection_dir):
    return_value = False

    try:
        full_file_pathname = os.path.join(collection_dir, "source.csv")
        reader = csv.DictReader(open(full_file_pathname, newline=""))

        print("")
        print("")
        print(">>> Endpoints found")
        print("-------------------")
        for row in reader:
            print(f'organisation: {row["organisation"]}')
            print(f'   pipelines: {row["pipelines"]}')
            print(f'    endpoint: {row["endpoint"]}')
            print("")

        return_value = True
    except:
        return_value = False
    finally:
        return return_value


def get_endpoints(organisation, pipeline_name, collection_dir):
    endpoint_hashes = []
    return_value = None

    try:
        full_file_pathname = os.path.join(collection_dir, "source.csv")
        reader = csv.DictReader(open(full_file_pathname, newline=""))

        for row in reader:
            if row["organisation"] == organisation and row["pipelines"] == pipeline_name:
                endpoint_hash = row["endpoint"]
                endpoint_hashes.append(endpoint_hash)

        return_value = endpoint_hashes
    except:
        return_value = None
    finally:
        return return_value


def download_endpoints(pipeline_name, endpoint_path, endpoint_hashes):
    return_value = False

    # create a collector
    collector = Collector(pipeline_name, collection_dir=None)
    # collector.log_dir = endpoint_path

    # download endpoints
    full_file_pathname = os.path.join(endpoint_path, "endpoint.csv")
    reader = csv.DictReader(open(full_file_pathname, newline=""))

    print("")
    print("")
    print(">>> Endpoint downloads")
    print("----------------------")
    for row in reader:
        endpoint = row["endpoint"]
        print(f"Checking endpoint: {endpoint}")
        if endpoint in endpoint_hashes:
            url = row["endpoint-url"]
            plugin = row.get("plugin", "")

            # skip manually added files ..
            if not url:
                continue

            print(f"   Using endpoint: {endpoint}")
            print(f"         with url: {url}")
            print(f"      with plugin: {plugin}")
            print("Downloading...")
            try:
                status = collector.fetch(
                    url,
                    endpoint=endpoint,
                    end_date=row.get("end-date", ""),
                    plugin=plugin,
                )

                return_value = True
            except:
                return_value = None
            finally:
                if status == FetchStatus.FAILED:
                    print(f"Download FAILED for: {url}")
                    print(f"        with status: {status}")
                else:
                    print(f"Download SUCCESS for {url}")

        return_value = True
        print("")

    return return_value


def endpoint_collection(collection_dir, endpoint_hashes):
    return_value = None

    # remove previously created log.csv and resouorce.csv files
    try:
        os.remove(Path(collection_dir) / "log.csv")
        os.remove(Path(collection_dir) / "resource.csv")
    except FileNotFoundError as exc:
        print("")
        print(">>> INFO: endpoint_collection:", exc)
    except OSError as exc:
        print("")
        print("ERROR: endpoint_collection:", exc)
        return_value = None

    collection = Collection(name=None, directory=collection_dir)
    collection.load()

    print("")
    print("")
    print(">>> Collection endpoint stats")
    print("-----------------------------")
    print(f"  Num source entries: {len(collection.source.entries)}")
    print(f"  Num source records: {len(collection.source.records)}")
    print("-----------------------------")
    print(f"Num endpoint entries: {len(collection.endpoint.entries)}")
    print(f"Num endpoint records: {len(collection.endpoint.records)}")
    print("-----------------------------")
    print(f"Num resource entries: {len(collection.resource.entries)}")
    print(f"Num resource records: {len(collection.resource.records)}")
    print("-----------------------------")
    print(f"     Num log entries: {len(collection.log.entries)}")
    print(f"     Num log records: {len(collection.log.records)}")

    #  there is no direct way to filter which logs/resources are saved to the csv
    #  this was originally built to do all collections at once
    #  we now manually filter the entries for logs/resources using the endpoint hashes made above to achieve this
    for endpoint in endpoint_hashes:
        log_entries = [
            entry for entry in collection.log.entries if entry["endpoint"] == endpoint
        ]
        resource_entries = [
            entry for entry in collection.resource.entries if entry["endpoints"] == endpoint
        ]

    collection.log.entries = log_entries
    collection.resource.entries = resource_entries
    collection.save_csv()

    print("")
    print("")
    print(">>> Items added to Collection")
    print("-----------------------------")
    print(f"Num resource entries: {len(collection.resource.entries)}")
    print("-----------------------------")
    print(f"     Num log entries: {len(collection.log.entries)}")

    print(">>>>", collection.resource.entries)

    return log_entries, resource_entries


def run_collection_pipeline(collection_dir, pipeline_name, dataset_name, resource_entries, log_entries):
    print("")

    # for each of the files
    # define additiionial files
    for resource in resource_entries:
        input_path = f"{collection_dir}resource/{resource['resource']}"
        output_path = f"{collection_dir}transformed/{dataset_name}/{resource['resource']}.csv"

        print(f">>> input_path: {input_path}")
        print(f">>> output_path:{output_path}")

