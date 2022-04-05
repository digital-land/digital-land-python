from datetime import datetime
from .phase import Phase


class ResourceDefaultPhase(Phase):
    """
    default organisation, prefix and entry-date values from the resource
    """

    def __init__(self, issues, collection):
        self.issues = issues
        self.collection = collection
        self.default_values = {}

    def set_resource_defaults(self, resource):
        resource_entry = self.collection.resource.records[resource][0]
        resource_organisations = self.collection.resource_organisations(resource)

        # a resource may hace been published by several organisations
        if len(resource_organisations) == 1:
            self.default_values["organisation"] = resource_organisations[0]

        # default entry-date from first time the resource was collected
        self.default_values["entry-date"] = resource_entry["start-date"]

    def process(self, stream):
        current_resource = None

        for block in stream:
            row = block["row"]
            resource = block["resource"]

            if resource != current_resource:
                self.set_resource_defaults(resource)
                current_resource = resource

            for field in ["organisation", "entry-date"]:
                if not row.get(field, "") and field in self.default_values:
                    self.issues.log_issue(field, "resource default", row[field])
                    row[field] = self.default_values[field]

            entry_date = datetime.strptime(row["entry-date"][:10], "%Y-%m-%d").date()
            if entry_date > datetime.today().date():
                self.issues.log_issue(field, "future entry-date", entry_date)
                row["entry-date"] = self.default_values["entry-date"]

            yield block
