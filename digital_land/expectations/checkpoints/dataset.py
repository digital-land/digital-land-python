import os
import json

from pathlib import Path

from .base import BaseCheckpoint
from ..utils import QueryRunner
from ..expectation_functions.sqlite import (
    check_old_entities,
    check_json_field_is_not_blank,
)

BASE = [
    {
        "function": check_old_entities,
        "name": "Check for retired entities in the entity table",
        "description": "Check for old entities",
        "severity": "warning",
        "responsbility": "internal",
    }
]

TYPOLOGY = {
    "document": [
        {
            "function": check_json_field_is_not_blank,
            "name": "Check entities in a document dataset have a document field",
            "severity": "warning",
            "responsibility": "internal",
            "field": "document-url",
        }
    ],
}

DATASET = {
    "article-4-direction-area": [
        {
            "function": check_json_field_is_not_blank,
            "name": "Check article 4 direction area has an associated article 4 direction",
            "severity": "warning",
            "responsibility": "internal",
            "field": "article-4-direction",
        }
    ]
}


class DatasetCheckpoint(BaseCheckpoint):
    def __init__(self, dataset_path, typology, dataset=None):

        super().__init__("dataset", dataset_path)
        self.dataset_path = Path(dataset_path)
        if dataset:
            self.dataset = dataset
        else:
            self.dataset = self.dataset_path.stem
        self.typology = typology

    def get_rule_orgs(rule: dict):
        """
        for each rule we need to get a list of the organisations that the rule
        applies to this is a semi colon separated list of individual orgs, org datasets
        or org prefixes which are a key of the inputted dict

        Args:
            - rule: a single expectation rule
        """

    def parse_rule(self, rule, org=None):
        """
        Can turn  a rrule into an expectation given an org it will format text strings
        """
        expectation = {}
        # set the operation riase error if it doesn't exist
        if org:
            operation = self.operation_factory(rule["operation"])
            expectation["operation"] = operation
            expectation["name"] = rule["name"].format(organisation=org)
            expectation["description"] = rule["description"].format(organisation=org)
            expectation["params"] = json.load(rule["params"].format(organisation=org))
            # this checkpoint uses sqlite query runner. May want to change in the future
            expectation["params"]["query_runner"] = QueryRunner(self.dataset_path)

    def load(self, rules):
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

    # def run_expectation(self, expectation):
    #     """
    #     runs a given expectation.
    #     """
    #     passed, msg, details = expectation["operation"](**expectation["params"])
    #     # set some core attributes
    #     if getattr(self, "responses", None):
    #         entry_date = self.entry_date
    #     else:
    #         now = datetime.now()
    #         entry_date = now.isoformat()
    #     arguments = {**kwargs}

    #     # Make a hash of this expectation, for now combine checkpoint name,
    #     # expectation name and the function name. Might want to adjust in future
    #     expectation_hash = hashlib.md5(
    #         self.checkpoint.encode()
    #         + self.data_name.encode()
    #         + expectation["function"].__name__.encode()
    #     ).hexdigest()

    #     # validate the errors, this will stop functions from being made that
    #     # don't conform to the right error values
    #     validated_issues = []
    #     for issue in issues:
    #         issue_class = issue_factory(issue["scope"])
    #         validated_issues.append(
    #             issue_class(**issue, expectation_result=expectation_hash)
    #         )

    #     return ExpectationResult(
    #         expectation_result=expectation_hash,
    #         checkpoint=self.checkpoint,
    #         entry_date=entry_date,
    #         name=expectation["name"],
    #         # description is optional
    #         description=arguments.get("description", None),
    #         severity=expectation["severity"],
    #         passed=passed,
    #         message=msg,
    #         issues=validated_issues,
    #         # not convinced we need the below but leave in for now
    #         data_name=self.data_name,
    #         # data_path=self.data_path,
    #     )

    def run(self):
        # TODO do somewhere different but not sure how
        # now = datetime.now()
        # self.entry_date = now.isoformat()
        self.failed_expectation_with_error_severity = 0

        for expectation in self.expectations:
            response = self.run_expectation(expectation)
            self.responses.append(response)
            self.issues.extend(response.issues)
            self.failed_expectation_with_error_severity += response.act_on_failure()

        # if self.failed_expectation_with_error_severity > 0:
        #     # raise DataQualityException(
        #     #     "One or more expectations with severity RaiseError failed, see results for more details"
        #     # )

    def save(self, output_dir, format="csv"):
        responses_file_path = os.path.join(
            output_dir, self.checkpoint, f"{self.data_name}-results.csv"
        )
        issues_file_path = os.path.join(
            output_dir, self.checkpoint, f"{self.data_name}-issues.csv"
        )

        self.save_results(
            self.responses,
            responses_file_path,
            format=format,
        )

        self.save_issues(self.issues, issues_file_path, format=format)
