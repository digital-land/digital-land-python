from pathlib import Path
from datetime import datetime
import os
import json
import hashlib

from csv import DictWriter

from ..response import ExpectationResponse
from ..exception import DataQualityException
from ..issue import issue_factory


class BaseCheckpoint:
    def __init__(self, checkpoint, data_path):
        self.checkpoint = checkpoint
        self.data_path = data_path
        self.data_name = Path(data_path).stem
        self.responses = []
        self.issues = []
        # each issue is going to have different fields, so define here what all of them are
        # this will take some iterations to get right
        self.response_fieldnames = [
            "response-id",
            "result",
            "message",
            "severity",
            "responsibility",
            "checkpoint",
            "data-name",
        ]
        self.issue_fieldnames = [
            "response-id",
            "scope",
            "message",
            "dataset",
            "organisation",
            "table-name",
            "field-name",
            "row-id",
            "rows",
            "row",
            "value",
        ]

    def load():
        """filled in by child classes, ensures a config is loaded correctly should raise error if not"""
        pass

    def save(self, output_dir, format="csv"):
        """filled in by child classes, uses save functions to save the data. could add default behaviour at somepoint"""
        pass

    def run_expectation(self, expectation):
        """
        runs a given expectation.
        """

        # kwargs passed tot he function cannot have any of the below names
        non_kwargs = ["function", "name", "description", "severity", "responsibility"]
        kwargs = {
            key: value for (key, value) in expectation.items() if key not in non_kwargs
        }

        result, msg, issues = expectation["function"](**kwargs)

        # set some core attributes
        if getattr(self, "responses", None):
            entry_date = self.entry_date
        else:
            now = datetime.now()
            entry_date = now.isoformat()
        arguments = {**kwargs}

        # Make a hash of this expectation, for now combine checkpoint name,
        # expectation name and the function name. Might want to adjust in future
        expectation_hash = hashlib.md5(
            self.checkpoint.encode()
            + self.data_name.encode()
            + expectation["function"].__name__.encode()
        ).hexdigest()

        # validate the errors, this will stop functions from being made that
        # don't conform to the right error values
        validated_issues = []
        for issue in issues:
            issue_class = issue_factory(issue["scope"])
            validated_issues.append(issue_class(**issue, response_id=expectation_hash))

        return ExpectationResponse(
            response_id=expectation_hash,
            checkpoint=self.checkpoint,
            entry_date=entry_date,
            name=expectation["name"],
            # description is optional
            description=arguments.get("description", None),
            severity=expectation["severity"],
            result=result,
            message=msg,
            issues=validated_issues,
            # not convinced we need the below but leave in for now
            data_name=self.data_name,
            # data_path=self.data_path,
        )

    def run(self):
        # TODO do somewhere different but not sure how
        now = datetime.now()
        self.entry_date = now.isoformat()
        self.failed_expectation_with_error_severity = 0

        for expectation in self.expectations:
            response = self.run_expectation(expectation)
            self.responses.append(response)
            self.issues.extend(response.issues)
            self.failed_expectation_with_error_severity += response.act_on_failure()

        if self.failed_expectation_with_error_severity > 0:
            raise DataQualityException(
                "One or more expectations with severity RaiseError failed, see results for more details"
            )

    def save_responses(self, responses, file_path, format="csv"):

        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as f:
            if format == "csv":
                dictwriter = DictWriter(f, fieldnames=self.response_fieldnames)
                dictwriter.writeheader()
                dictwriter.writerows(
                    [response.dict_for_export() for response in responses]
                )
            elif format == "json":
                json.dump([response.to_dict() for response in responses], f)
            else:
                raise ValueError(f"format must be csv or json and cannot be {format}")

    def save_issues(self, issues, file_path, format="csv"):
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as f:
            if format == "csv":
                dictwriter = DictWriter(f, fieldnames=self.issue_fieldnames)
                dictwriter.writeheader()
                dictwriter.writerows([issue.to_dict() for issue in issues])
            elif format == "json":
                json.dump([issue.to_dict() for issue in issues], f)
            else:
                raise ValueError(f"format must be csv or json and cannot be {format}")

    def act_on_critical_error(self, failed_expectation_with_error_severity=None):
        if failed_expectation_with_error_severity is None:
            getattr(self, "failed_expectation_with_error_severity", None)

        if failed_expectation_with_error_severity:
            if failed_expectation_with_error_severity > 0:
                raise DataQualityException(
                    "One or more expectations with severity RaiseError failed, see results for more details"
                )
