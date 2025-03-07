#!/usr/bin/env python3

#
#  create the dependencies between collected resources and a dataset
#  saved as collection/pipeline.mk
#


import json
from datetime import datetime


def transformed_path(resource, dataset):
    return "$(TRANSFORMED_DIR)" + dataset + "/" + resource + ".csv"


def dataset_path(dataset):
    return "$(DATASET_DIR)" + dataset + ".csv"


def filter_dataset_resource_map(dataset_resource, new_resources):
    return {
        dataset: {h for h in hashes if h in new_resources}
        for dataset, hashes in dataset_resource.items()
        if any(h in new_resources for h in hashes)
    }


def pipeline_makerules(collection, state_difference_path):
    dataset_resource = collection.dataset_resource_map()
    redirect = {}
    for entry in collection.old_resource.entries:
        redirect[entry["old-resource"]] = entry["resource"]
    sep = ""

    # load state differences to determine whether we need to reprocess everything, or just new resources
    with open(state_difference_path) as f:
        state_difference = json.load(f)

    reprocess_all = False
    if (
        "code" in state_difference["differences"]
        or "specification" in state_difference["differences"]
        or state_difference["incremental_loading_override"]
    ):
        reprocess_all = True
    else:
        # if we aren't reprocessing everything we need to select only new resources
        new_resources = collection.resources_started_on(
            datetime.utcnow().isoformat()[:10]
        )
        dataset_resource = filter_dataset_resource_map(dataset_resource, new_resources)
        print("dataset_resource: ", dataset_resource)

    # if the code/spec has changed we need to reprocess everything:

    # otherwise check for new resources and only use those
    for dataset in sorted(dataset_resource):
        print(sep, end="")
        sep = "\n\n"

        name_var = dataset.upper().replace("-", "_")
        dataset_var = name_var + "_DATASET"
        dataset_files_var = name_var + "_TRANSFORMED_FILES"

        print("%s=%s" % (dataset_var, dataset_path(dataset)))
        print("%s=" % (dataset_files_var), end="")
        for resource in sorted(dataset_resource[dataset]):
            if redirect.get(resource, resource):
                print("\\\n    %s" % (transformed_path(resource, dataset)), end="")
        print()

        for resource in sorted(dataset_resource[dataset]):
            old_resource = resource
            resource = redirect.get(resource, resource)
            # stops resources that have been removed
            if resource:
                # this should be the path to the resource being processed
                resource_path = collection.resource_path(resource)
                endpoints = " ".join(collection.resource_endpoints(old_resource))
                organisations = " ".join(
                    collection.resource_organisations(old_resource)
                )
                entry_date = collection.resource_start_date(old_resource)

                print(
                    "\n%s: %s"
                    % (
                        transformed_path(old_resource, dataset),
                        resource_path,
                    )
                )
                call_pipeline = (
                    "\t$(call run-pipeline,"
                    + f" --endpoints '{endpoints}'"
                    + f" --organisations '{organisations}'"
                    + f" --entry-date '{entry_date}'"
                )
                # we will include the resource arguement if the old resource
                # is  different so it's processed as the old_resource
                if resource != old_resource:
                    call_pipeline = call_pipeline + f" --resource '{old_resource}'"

                call_pipeline = call_pipeline + " )"

                print(call_pipeline)

        print("\n$(%s): $(%s)" % (dataset_var, dataset_files_var))
        if reprocess_all:
            print("\t$(build-dataset)")
        else:
            print("\t$(update-dataset)")
        print("\ntransformed:: $(%s)" % (dataset_files_var))
        print("\ndataset:: $(%s)" % (dataset_var))

    print("\n\nDATASETS=", end="")
    for dataset in sorted(dataset_resource):
        print("\\\n\t$(DATASET_DIR)%s.sqlite3" % (dataset), end="")
