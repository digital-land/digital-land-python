from pathlib import Path
from datetime import datetime

from .register import Item, Register


# fieldnames should come from, or be checked against the specification:
# https://digital-land.github.io/specification/schema/log/
class LogRegister(Register):
    register = "log"
    key = "endpoint"
    fieldnames = [
        "endpoint",
        "elapsed",
        "request-headers",
        "resource",
        "response-headers",
        "status",
        "entry-date",
        "start-date",
        "end-date",
    ]


# fieldnames should come from, or be checked against the specification:
# https://digital-land.github.io/specification/schema/source/
class EndpointRegister(Register):
    register = "endpoint"
    fieldnames = [
        "endpoint",
        "endpoint-url",
        "plugin",
        "parameters",
        "entry-date",
        "start-date",
        "end-date",
    ]


# fieldnames should come from, or be checked against the specification:
# https://digital-land.github.io/specification/schema/source/
class SourceRegister(Register):
    register = "source"
    key = "endpoint"
    fieldnames = [
        "source",
        "attribution",
        "collection",
        "documentation-url",
        "endpoint",
        "licence",
        "organisation",
        "pipeline",
        "status",
        "entry-date",
        "start-date",
        "end-date",
    ]


class ResourceRegister(Register):
    register = "resource"
    fieldnames = [
        "resource",
        "start-date",
    ]

    def load_from_log(self, log):
        resources = {}
        for entry in log.entries:
            if "resource" in entry.item:
                resource = entry.item["resource"]
                if resource in resources:
                    resources[resource]["start-date"] = min(
                        resources[resource]["start-date"], entry.item["entry-date"]
                    )
                else:
                    resources[resource] = {"start-date": entry.item["entry-date"]}

        for key, resource in sorted(resources.items()):
            self.add(
                Item(
                    {
                        "resource": key,
                        "start-date": datetime.fromisoformat(
                            resource["start-date"]
                        ).strftime("%Y-%m-%d"),
                    }
                )
            )


# this is a DataPackage ..
class Collection:
    def __init__(self, directory="collection/"):
        self.directory = directory
        self.source = SourceRegister(self.dirname)
        self.endpoint = EndpointRegister(self.dirname)
        self.log = LogRegister(self.dirname)
        self.resource = ResourceRegister(self.dirname)

    def load(self):
        self.log.load_collection()
        self.endpoint.load()
        self.source.load()
        self.resource.load_from_log(self.log)

    def resources(self, pipeline=None):
        # TODO is pipeline param needed?
        return sorted(self.resource.record)

    def resource_organisation(self, resource):
        "return the list of organisations for which a resource was collected"
        endpoints = {}
        organisations = {}

        # entries which collected the resource
        for entry in self.log.entries:
            if "resource" in entry.item and entry.item["resource"] == resource:
                endpoints[entry.item["endpoint"]] = True

        # sources which cite the endpoint
        for endpoint in endpoints:
            for n in self.source.record[endpoint]:
                organisations[self.source.entries[n].item["organisation"]] = True
        return sorted(organisations)

    def resource_path(self, resource):
        return Path(self.directory) / "resource" / resource
