#!/usr/bin/env python3

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
    redirect = {}
    for entry in collection.old_resource.entries:
        redirect[entry["old-resource"]] = entry["resource"]
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
                    call_pipeline = call_pipeline + f"--resource {old_resource}"

                call_pipeline = call_pipeline + ")"

                print(call_pipeline)

        print("\n$(%s): $(%s)" % (dataset_var, dataset_files_var))
        print("\t$(build-dataset)")
        print("\ntransformed:: $(%s)" % (dataset_files_var))
        print("\ndataset:: $(%s)" % (dataset_var))

    print("\n\nDATASETS=", end="")
    for dataset in sorted(dataset_resource):
        print("\\\n\t$(DATASET_DIR)%s.sqlite3" % (dataset), end="")
