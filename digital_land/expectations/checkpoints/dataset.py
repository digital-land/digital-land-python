import json
import spatialite
from pathlib import Path
from jinja2 import Template

from digital_land.organisation import Organisation

from .base import BaseCheckpoint
from ..log import ExpectationLog
from ..operation import (
    count_lpa_boundary,
    count_deleted_entities,
    duplicate_geometry_check,
)


class DatasetCheckpoint(BaseCheckpoint):
    def __init__(self, dataset, file_path, organisations: Organisation):

        self.dataset = dataset
        self.dataset_path = Path(file_path)
        self.organisations = organisations
        self.log = ExpectationLog(dataset=dataset)

    def operation_factory(self, operation_string: str):
        """
        conevrts a string into an operation, available operations are specific
        to the checkpoint

        Args
            operation: a string representing an operation
        """
        operation_map = {
            "count_lpa_boundary": count_lpa_boundary,
            "count_deleted_entities": count_deleted_entities,
            "duplicate_geometry_check": duplicate_geometry_check,
        }
        operation = operation_map[operation_string]
        return operation

    def get_rule_orgs(self, rule: dict) -> list:
        """
        for each rule we need to get a list of the organisations that the rule
        applies to this is a semi colon separated list of individual orgs, org datasets
        or org prefixes which are a key of the inputted dict

        Args:
            - rule: a single expectation rule
        """
        final_rule_orgs = []
        for org in rule["organisations"].split(";"):
            org_lookup = self.organisations.lookup(org)
            if org_lookup:
                final_rule_orgs.append(self.organisations.get(org_lookup))
            else:
                # try get orgs by dataset
                dataset_orgs = self.organisations.get_orgs_by_dataset(org)
                if dataset_orgs:
                    final_rule_orgs.extend(dataset_orgs)
                else:
                    raise ValueError(
                        f"Cannot attribute organisations to the provided value {org}"
                    )

        return [
            {key.replace("-", "_"): value for key, value in rule_org.items()}
            for rule_org in final_rule_orgs
        ]

    def parse_rule(self, rule, org=None) -> dict:
        """
        turn a rule into an expectation given an org it will format text strings using jinja templating
        """
        expectation = {}
        # set the operation riase error if it doesn't exist
        if org:
            operation = self.operation_factory(rule["operation"])
            expectation["operation"] = operation
            expectation["name"] = Template(rule["name"]).render(organisation=org)
            expectation["description"] = Template(rule.get("description", "")).render(
                organisation=org
            )
            expectation["organisation"] = org
            expectation["dataset"] = self.dataset
            expectation["severity"] = rule.get("severity", "")
            expectation["responsibility"] = rule.get("responsibility", "")

            # params are different string needs to be rendered and then loaded from json
            expectation["parameters"] = json.loads(
                Template(rule["parameters"]).render(organisation=org)
            )
        else:
            operation = self.operation_factory(rule["operation"])
            expectation["operation"] = operation
            expectation["name"] = rule["name"]
            expectation["description"] = rule.get("description", "")
            expectation["organisation"] = ""
            expectation["dataset"] = self.dataset
            expectation["severity"] = rule.get("severity", "")
            expectation["responsibility"] = rule.get("responsibility", "")

            # params are different it's read in from a json, onlly format the values
            expectation["parameters"] = json.loads(
                rule["parameters"]
            )  # this loads params as a string, should it be json?

        return expectation

    def load(self, rules):
        """
        given a set of rules this function loads them into the checkpoint
        for the dataset checkpoint we antiipates rules contain organisations with which
        expectations need parsing
        """
        self.expectations = []
        for rule in rules:
            if rule["organisations"]:
                rule_orgs = self.get_rule_orgs(rule)

                for rule_org in rule_orgs:
                    expectation = self.parse_rule(rule, rule_org)
                    self.expectations.append(expectation)

            else:
                expectation = self.parse_rule(rule)
                self.expectations.append(expectation)

    def run_expectation(self, expectation) -> tuple:
        """
        runs a given expectation returning the result, description and message
        a log can be provided to record this information
        """
        params = expectation["parameters"]
        with spatialite.connect(self.dataset_path) as conn:
            print("expectation:", expectation)
            passed, msg, details = expectation["operation"](conn=conn, **params)

        return passed, msg, details

    def run(self):
        """
        run the set of expectations that have been loaded into the checkpoint
        results will be stoed in the log and can be saved used .save
        """
        # TODO implement faillure on critical errors, this is also the zombie code below
        # self.failed_expectation_with_error_severity = 0

        for expectation in self.expectations:
            passed, message, details = self.run_expectation(expectation)
            self.log.add(
                {
                    "organisation": expectation.get("organisation", "").get(
                        "organisation", ""
                    ),
                    "name": expectation["name"],
                    "passed": passed,
                    "message": message,
                    "details": details,
                    "description": expectation["description"],
                    "severity": expectation["severity"],
                    "responsibility": expectation["responsibility"],
                    "operation": expectation["operation"].__name__,
                    "parameters": expectation["parameters"],
                }
            )
            # self.failed_expectation_with_error_severity

        # if self.failed_expectation_with_error_severity > 0:
        #     # raise DataQualityException(
        #     #     "One or more expectations with severity RaiseError failed, see results for more details"
        #     # )

    def save(self, output_dir: Path):
        """
        save the outputs as a file, the file is named based the the dataset
        and stored in the provided directory
        """
        self.log.save_parquet(output_dir)
