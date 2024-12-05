from .phase import Phase

EXCLUDE_ISSUES = ["invalid geometry"]


class PivotPhase(Phase):
    """
    split entries into a series of facts
    """

    def __init__(self, issue_log=None):
        self.issue_map = {}
        if issue_log:
            for row in issue_log.rows:
                self.issue_map[(row["field"], row["entry-number"])] = row

    def process(self, stream):
        for block in stream:
            row = block["row"]
            for field, value in sorted(row.items()):
                if field in ["entity"]:
                    continue

                issue = self.issue_map.get((field, block["entry-number"]))

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
