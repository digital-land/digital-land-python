from .phase import Phase

EXCLUDE_ISSUES = [
    "invalid coordinates",
    "WGS84 out of bounds of England",
    "OSGB out of bounds of England",
]


class PivotPhase(Phase):
    """
    split entries into a series of facts
    """

    def __init__(self, issue_log=None):
        self.issue_log = issue_log

    def get_issue(self, field, entry_number):
        if self.issue_log:
            for row in self.issue_log.rows:
                # Hack for the GeoX,GeoY field in brownfield land
                if (
                    row["field"] == field
                    or (row["field"] == "GeoX,GeoY" and field == "point")
                ) and row["entry-number"] == entry_number:
                    return row
        return None

    def process(self, stream):
        for block in stream:
            row = block["row"]
            for field, value in sorted(row.items()):
                if field in ["entity"]:
                    continue

                issue = self.get_issue(field, block["entry-number"])

                if issue and issue["issue-type"] in EXCLUDE_ISSUES:
                    continue

                block["row"] = {
                    # fact
                    "fact": "",
                    "entity": row.get("entity", ""),
                    "field": field,
                    "value": value,
                    # entry
                    "priority": block["priority"],
                    "resource": block["resource"],
                    "line-number": block["line-number"],
                    "entry-number": block["entry-number"],
                    "entry-date": row["entry-date"],
                }

                yield block
