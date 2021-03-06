#!/usr/bin/env python3

import os
from pathlib import Path
from datetime import datetime


def transformed_path(resource, pipeline):
    return "$(TRANSFORMED_DIR)" + pipeline + "/" + resource + ".csv"


def dataset_path(pipeline):
    return "$(DATASET_DIR)" + pipeline + ".csv"


def pipeline_makerules(collection):
    today = datetime.utcnow().isoformat()

    endpoint_pipeline = {}
    pipeline_resource = {}

    for entry in collection.source.entries:
        if entry["end-date"] and entry["end-date"] > today:
            continue

        endpoint_pipeline.setdefault(entry["endpoint"], set())
        for pipeline in entry["pipelines"].split(";"):
            if pipeline:
                endpoint_pipeline[entry["endpoint"]].add(pipeline)

    for entry in collection.resource.entries:
        if entry["end-date"] and entry["end-date"] > today:
            continue

        for endpoint in entry["endpoints"].split(";"):
            for pipeline in endpoint_pipeline[endpoint]:
                pipeline_resource.setdefault(pipeline, set())
                pipeline_resource[pipeline].add(entry["resource"])

    sep = ""
    for pipeline in sorted(pipeline_resource):
        print(sep, end="")
        sep = "\n\n"

        pipeline_var = pipeline.upper().replace("-", "_")
        dataset_var = pipeline_var + "_DATASET"
        dataset_files_var = pipeline_var + "_TRANSFORMED_FILES"

        print("%s=%s" % (dataset_var, dataset_path(pipeline)))
        print("%s=" % (dataset_files_var), end="")
        for resource in sorted(pipeline_resource[pipeline]):
            print("\\\n    %s" % (transformed_path(resource, pipeline)), end="")
        print()

        for resource in sorted(pipeline_resource[pipeline]):

            fixed_path = Path("fixed") / (resource + ".csv")
            resource_path = collection.resource_path(resource)
            resource_path = fixed_path if os.path.isfile(fixed_path) else resource_path

            print(
                "\n%s: %s"
                % (
                    transformed_path(resource, pipeline),
                    resource_path,
                )
            )
            print("\t$(run-pipeline)")

        print("\n$(%s): $(%s)" % (dataset_var, dataset_files_var))
        print("\t$(build-dataset)")
        print("\ntransformed:: $(%s)" % (dataset_files_var))
        print("\ndataset:: $(%s)" % (dataset_var))
