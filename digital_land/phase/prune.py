from .phase import Phase
import logging


class EntityPrunePhase(Phase):
    """
    remove entries with a missing entity
    """

    def __init__(self, issues=None):
        self.issues = issues

    def process(self, stream):
        for block in stream:
            row = block["row"]
            if not row.get("entity", ""):
                resource = block["resource"]
                prefix = row.get("prefix", "")
                reference = row.get("reference", "")
                curie = f"{prefix}:{reference}"
                line_number = block["line-number"]

                if self.issues:
                    self.issues.log_issue(
                        "entity", "missing lookup", curie, line_number=line_number
                    )

                logging.info(
                    f"{resource} line {line_number}: missing entity for {curie}"
                )
                logging.debug(block)
                continue

            yield block


class FactPrunePhase(Phase):
    """
    remove facts with a missing value
    """

    def process(self, stream):
        for block in stream:
            row = block["row"]

            if not row.get("value", ""):
                continue

            yield block
