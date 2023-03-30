import csv
import os

from tests.e2e.debug_pipline.helpers.collect import Collector


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
                collector.fetch(
                    url,
                    endpoint=endpoint,
                    end_date=row.get("end-date", ""),
                    plugin=plugin,
                )

                return_value = True
            except:
                return_value = None

        print("")

    return return_value

