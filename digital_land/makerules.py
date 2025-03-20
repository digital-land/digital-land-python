#!/usr/bin/env python3

#
#  create the dependencies between collected resources and a dataset
#  saved as collection/pipeline.mk
#


from enum import Enum

from digital_land.state import compare_state
from .state import State


class ProcessingOption(Enum):
    PROCESS_ALL = "all"
    PROCESS_PARTIAL = "partial"
    PROCESS_NONE = "none"


def transformed_path(resource, dataset):
    return "$(TRANSFORMED_DIR)" + dataset + "/" + resource + ".csv"


def dataset_path(dataset):
    return "$(DATASET_DIR)" + dataset + ".csv"


def get_processing_option(
    collection,
    specification_dir,
    pipeline_dir,
    resource_dir,
    incremental_loading_override,
    state_path,
):
    # If there's no previous state, process everything
    if not state_path:
        return ProcessingOption.PROCESS_ALL

    # Compare current state with the previous state
    diffs = compare_state(
        specification_dir,
        collection.dir,
        pipeline_dir,
        resource_dir,
        incremental_loading_override,
        state_path,
    )

    diffs = diffs or []  # handle if diffs is None

    # If incremental loading is overridden or critical configs changed, process everything
    critical_changes = {"code", "pipeline", "collection", "specification"}
    if incremental_loading_override or critical_changes & set(diffs):
        return ProcessingOption.PROCESS_ALL

    # New resources downloaded
    if "resource" in diffs:
        return ProcessingOption.PROCESS_PARTIAL

    if not diffs:
        return ProcessingOption.PROCESS_NONE

    # If there are diffs we don't recognise then play safe and reprocess everything
    return ProcessingOption.PROCESS_ALL


def pipeline_makerules(
    collection,
    specification_dir,
    pipeline_dir,
    resource_dir,
    incremental_loading_override,
    state_path=None,
):
    dataset_resource = collection.dataset_resource_map()
    process = get_processing_option(
        collection,
        specification_dir,
        pipeline_dir,
        resource_dir,
        incremental_loading_override,
        state_path,
    )

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
            if redirect.get(resource, resource):
                print("\\\n    %s" % (transformed_path(resource, dataset)), end="")
        print()

        if process == ProcessingOption.PROCESS_NONE:
            print("\n$(%s)::" % dataset_var)
            print('\techo "No state change and no new resources to transform"')
            print("\ntransformed::")
            print('\techo "No state change and no new resources to transform"')
            print("\ndataset::")
            print('\techo "No state change so no resources have been transformed"')
            continue

        if state_path is not None:
            latest_state = State.load(state_path)
            if "last_updated_date" in latest_state.keys():
                last_updated_date = latest_state["last_updated_date"]
            else:
                last_updated_date = None
        else:
            last_updated_date = None
        # process = ProcessingOption.PROCESS_PARTIAL
        # last_updated_date = "2025-03-18"
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

                if (
                    process == ProcessingOption.PROCESS_ALL
                    or last_updated_date is None
                    or (
                        process == ProcessingOption.PROCESS_PARTIAL
                        and entry_date > last_updated_date
                    )
                ):
                    print(
                        "\n%s: %s"
                        % (
                            transformed_path(old_resource, dataset),
                            resource_path,
                        )
                    )

                    call_pipeline = (
                        "\t$(call run-pipeline,"
                        + " --endpoints '%s'" % endpoints
                        + " --organisations '%s'" % organisations
                        + " --entry-date '%s'" % entry_date
                    )
                    # we will include the resource argument if the old resource
                    # is different so it's processed as the old_resource
                    if resource != old_resource:
                        call_pipeline = call_pipeline + f" --resource '{old_resource}'"

                    call_pipeline = call_pipeline + " )"

                    print(call_pipeline)

        print("\n$(%s): $(%s)" % (dataset_var, dataset_files_var))
        if process == ProcessingOption.PROCESS_PARTIAL:
            print("\t$(update-dataset)")
        else:
            print("\t$(build-dataset)")
        print("\ntransformed:: $(%s)" % (dataset_files_var))
        print("\ndataset:: $(%s)" % (dataset_var))

    print("\n\nDATASETS=", end="")
    for dataset in sorted(dataset_resource):
        print("\\\n\t$(DATASET_DIR)%s.sqlite3" % (dataset), end="")
