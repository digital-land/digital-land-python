from datetime import datetime


def get_pipeline_resource_mapping_for_collection(collection):
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
    return pipeline_resource
