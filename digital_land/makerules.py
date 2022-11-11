#!/usr/bin/env python3

import os
from pathlib import Path


#
#  create the dependencies between collected resources and a dataset
#  saved as collection/pipeline.mk
#


def transformed_path(resource, dataset):
    return "$(TRANSFORMED_DIR)" + dataset + "/" + resource + ".csv"


def dataset_path(dataset):
    return "$(DATASET_DIR)" + dataset + ".csv"


def pipeline_makerules(collection):
    dataset_resource = collection.dataset_resource_map()
    sep = ""
    for dataset in sorted(dataset_resource):
        print(sep, end="")
        sep = "\n\n"

        name_var = dataset.upper().replace("-", "_")
        dataset_var = name_var + "_DATASET"
        dataset_files_var = name_var + "_TRANSFORMED_FILES"

        print("%s=%s" % (dataset_var, dataset_path(dataset)))
        print("%s=" % (dataset_files_var), end="")
        for resource in sorted(dataset_resource[dataset]):
            print("\\\n    %s" % (transformed_path(resource, dataset)), end="")
        print()

        for resource in sorted(dataset_resource[dataset]):
            fixed_path = Path("fixed") / (resource + ".csv")
            resource_path = collection.resource_path(resource)
            resource_path = fixed_path if os.path.isfile(fixed_path) else resource_path
            endpoints = " ".join(collection.resource_endpoints(resource))
            organisations = " ".join(collection.resource_organisations(resource))
            entry_date = collection.resource_start_date(resource)

            print(
                "\n%s: %s"
                % (
                    transformed_path(resource, dataset),
                    resource_path,
                )
            )
            print(
                "\t$(call run-pipeline,"
                + f" --endpoints '{endpoints}'"
                + f" --organisations '{organisations}'"
                + f" --entry-date '{entry_date}')"
            )

        print("\n$(%s): $(%s)" % (dataset_var, dataset_files_var))
        print("\t$(build-dataset)")
        print("\ntransformed:: $(%s)" % (dataset_files_var))
        print("\ndataset:: $(%s)" % (dataset_var))

    print("\n\nDATASETS=", end="")
    for dataset in sorted(dataset_resource):
        print("\\\n\t$(DATASET_DIR)%s.sqlite3" % (dataset), end="")
