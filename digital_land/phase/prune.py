from .phase import Phase
import logging


class EntityPrunePhase(Phase):
    """
    remove entries with a missing entity
    """

    def __init__(self, issues=None):
        self.issues = issues

    def process(self, reader):
        for stream_data in reader:
            row = stream_data["row"]
            if not row.get("entity", ""):
                resource = stream_data["resource"]
                prefix = row.get("prefix", "")
                reference = row.get("reference", "")
                curie = f"{prefix}:{reference}"
                line_number = stream_data["line-number"]

                if self.issues:
                    self.issues.log_issue(
                        "entity", "missing lookup", curie, line_number=line_number
                    )

                logging.info(
                    f"{resource} line {line_number}: missing entity for {curie}"
                )
                logging.debug(stream_data)
                continue

            yield stream_data


class FactPrunePhase(Phase):
    """
    remove facts with a missing value
    """

    def process(self, reader):
        for stream_data in reader:
            row = stream_data["row"]

            if not row.get("value", ""):
                continue

            yield stream_data
