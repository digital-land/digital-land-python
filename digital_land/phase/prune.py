from .phase import Phase
import logging
import warnings


class FieldPrunePhase(Phase):
    """
    reduce fields to just those specified for the dataset
    """

    def __init__(self, fields):
        self.fields = list(
            set(fields + ["entity", "organisation", "prefix", "reference"])
        )
        logging.debug(f"pruning fields to {self.fields}")

    def process(self, stream):
        for block in stream:
            row = block["row"]

            o = {}
            for field in self.fields:
                o[field] = row.get(field, "")

            block["row"] = o
            yield block


class EntityPrunePhase(Phase):
    """
    remove entries with a missing entity
    """

    def __init__(self, issue_log=None, dataset_resource_log=None):
        self.issues = issue_log
        if issue_log is not None:
            warnings.warn(
                "The 'issue_log' parameter is deprecated.", DeprecationWarning
            )
        self.log = dataset_resource_log

    def process(self, stream):
        entry_count = 0
        for block in stream:
            row = block["row"]
            if not row.get("entity", ""):
                resource = block["resource"]
                prefix = row.get("prefix", "")
                reference = row.get("reference", "")
                curie = f"{prefix}:{reference}"
                entry_number = block["entry-number"]

                logging.info(
                    f"{resource} row {entry_number}: missing entity for {curie}"
                )
                logging.debug(block)
                continue

            entry_count = entry_count + 1
            yield block

        self.log.entry_count = entry_count


class FactPrunePhase(Phase):
    """
    remove facts with a missing value
    """

    def process(self, stream):
        for block in stream:
            row = block["row"]

            if not row.get("value", "") and row.get("field", "") != "end-date":
                continue

            yield block
