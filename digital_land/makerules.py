#!/usr/bin/env python3

#
#  create the dependencies between collected resources and a dataset
#  saved as collection/pipeline.mk
#


from enum import Enum
import logging
from digital_land.state import compare_state

logger = logging.getLogger(__name__)


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
    critical_changes = {"pipeline", "collection", "specification"}
    # {"code", "pipeline", "collection", "specification"}
    # When testing on dev will want to use the following critical_changes as code is likely to always change
    # {"pipeline", "collection", "specification"}
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
    logger.setLevel(logging.INFO)
    process = get_processing_option(
        collection,
        specification_dir,
        pipeline_dir,
        resource_dir,
        incremental_loading_override,
        state_path,
    )
    # # Hard coded for testing purposes  REMOVE LATER
    # if process == ProcessingOption.PROCESS_PARTIAL:
    #     process = ProcessingOption.PROCESS_ALL
    # Hard code process value, REMOVE LATER
    process = ProcessingOption.PROCESS_PARTIAL
    logger.info(f"Process is: {process}")
    dataset_resource = collection.dataset_resource_map(process, state_path)
    redirect = {}
    for entry in collection.old_resource.entries:
        redirect[entry["old-resource"]] = entry["resource"]
    sep = ""
    logger.info(f"Length of dataset_resource: {len(dataset_resource)}")
    if not any(dataset_resource.values()):
        # If nothing to process then print messages
        print("\n::")
        print('\techo "No state change and no new resources to transform"')
        print("\ntransformed::")
        print('\techo "No state change and no new resources to transform"')
        print("\ndataset::")
        print('\techo "No state change so no resources have been transformed"')
    else:
        # pipeline commands for new resources (for ProcessingOption.PROCESS_PARTIAL)
        # or ProcessingOption.PROCESS_ALL
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

            # # Hard coding for testing purposes
            process = ProcessingOption.PROCESS_PARTIAL
            logger.info(f"process is: {process}")
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

        for dataset, value in sorted(dataset_resource.items()):
            # Only process datasets with values in them
            if value:
                print("\\\n\t$(DATASET_DIR)%s.sqlite3" % (dataset), end="")
