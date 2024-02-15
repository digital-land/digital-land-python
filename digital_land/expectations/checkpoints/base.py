from pathlib import Path
from itertools import chain
from datetime import datetime
import os
import json
import hashlib

from csv import DictWriter

from ..response import ExpectationResponse
from ..exception import DataQualityException


class BaseCheckpoint:
    def __init__(self, checkpoint, data_path):
        self.checkpoint = checkpoint
        self.data_path = data_path
        self.data_name = Path(data_path).stem

    def load():
        """filled in by child classes, ensures a config is loaded correctly should raise error if not"""
        pass

    def save(self, output_dir, format="csv"):
        self.save_responses(
            self.responses,
            os.path.join(output_dir, self.checkpoint),
            format=format,
        )

    def run_expectation(self, expectation_function, **kwargs):
        """
        runs a given function with the kwargs
        """
        #  = {**kwargs}
        # expectation_function = getattr(expectations, expectation[""])
        # TODO add an errors return detail below
        result, msg, errors = expectation_function(**kwargs)

        if getattr(self, "responses", None):
            entry_date = self.entry_date
        else:
            now = datetime.now()
            entry_date = now.isoformat()
        arguments = {**kwargs}

        # Make a hash of this expecation
        expectation = hashlib.md5(
            self.checkpoint.encode()
            + self.entry_date.encode()
            + expectation_function.__name__.encode()
        ).hexdigest()

        return ExpectationResponse(
            run=hashlib.md5(
                self.checkpoint.encode() + self.data_name.encode() + entry_date.encode()
            ).hexdigest(),
            checkpoint=self.checkpoint,
            entry_date=entry_date,
            name=arguments.get("name", expectation_function.__name__),
            description=arguments.get("description", None),
            expectation=expectation,
            severity=arguments["severity"],
            result=result,
            msg=msg,
            errors=errors,
            data_name=self.data_name,
            data_path=self.data_path,
        )

    # should be decided by the actualy checkpoint
    def run(self):

        self.responses = []

        # TODO do somewhere different but not sure how
        now = datetime.now()
        self.entry_date = now.isoformat()
        self.failed_expectation_with_error_severity = 0

        for expectation, kwargs in self.expectations.items():
            response = self.run_expectation(expectation, **kwargs)
            self.responses.append(response)
            self.failed_expectation_with_error_severity += response.act_on_failure()

        if self.failed_expectation_with_error_severity > 0:
            raise DataQualityException(
                "One or more expectations with severity RaiseError failed, see results for more details"
            )

    def save_responses(self, responses, results_base, format="csv"):

        # Assign the appropriate expectation to the errors
        all_errors = []  # List of dicts
        for response in responses:
            for error in response.errors:
                error = error.to_dict()
                error["expectation"] = response.expectation
                all_errors.append(error)

        # The docs seems to suggest you can pass exclude=.. to to_dict but it doesn't work, so
        # map the resulting dict instead.
        def remove_errors_column(d):
            if "errors" in d.keys():
                del d["errors"]
            return d

        responses_as_dicts = map(
            remove_errors_column, [response.to_dict() for response in responses]
        )

        results_fieldnames = [
            x for x in responses[0].__annotations__.keys() if x != "errors"
        ]

        results_path = results_base + os.extsep + format
        os.makedirs(os.path.dirname(results_path), exist_ok=True)
        with open(results_path, "w") as f:
            if format == "csv":
                dictwriter = DictWriter(f, fieldnames=results_fieldnames)
                dictwriter.writeheader()
                dictwriter.writerows(responses_as_dicts)
            elif format == "json":
                json.dump(responses_as_dicts, f)
            else:
                raise ValueError(f"format must be csv or json and cannot be {format}")

        # Build the filednames for the errors table
        errors_fieldnames = ["expectation", "message"]
        for fieldname in chain.from_iterable(
            [[keys for keys in dict.keys()] for dict in all_errors]
        ):
            if fieldname not in errors_fieldnames:
                errors_fieldnames.append(fieldname)

        errors_path = results_base + "-errors" + os.extsep + format
        with open(errors_path, "w") as f:
            if format == "csv":
                dictwriter = DictWriter(f, fieldnames=errors_fieldnames)
                dictwriter.writeheader()
                dictwriter.writerows(all_errors)
            elif format == "json":
                json.dump(all_errors, f)
            else:
                raise ValueError(f"format must be csv or json and cannot be {format}")

    # feels not needed
    # def act_on_critical_error(self, failed_expectation_with_error_severity=None):
    #     if failed_expectation_with_error_severity is None:
    #         getattr(self, "failed_expectation_with_error_severity", None)

    #     if failed_expectation_with_error_severity:
    #         if failed_expectation_with_error_severity > 0:
    #             raise DataQualityException(
    #                 "One or more expectations with severity RaiseError failed, see results for more details"
    #             )
