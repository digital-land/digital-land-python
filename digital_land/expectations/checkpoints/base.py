from pathlib import Path
from itertools import chain
from datetime import datetime
import os
import json

from csv import DictWriter

from ..response import ExpectationResponse
from ..exception import DataQualityException


class BaseCheckpoint:
    def __init__(self, data_path):
        # self.results_file_path = results_file_path
        self.data_path = data_path
        self.data_name = Path(data_path).stem
        # self.query_runner = QueryRunner(self.data_path)

    def load():
        """filled in by child classes, ensures a config is loaded correctly should raise error if not"""
        pass

    def save(self, output_dir, format="csv"):
        self.save_responses(
            self.responses,
            os.path.join(output_dir, self.__class__.__name__ + ".csv"),
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
        # TODO return errors and a response
        response = ExpectationResponse(
            entry_date=entry_date,
            name=arguments["name"],
            description=arguments.get("description", None),
            # TODO this won't work and should change it to function and get the name
            # of the function above
            expectation=expectation_function.__name__,
            severity=arguments["severity"],
            result=result,
            msg=msg,
            errors=errors,
            data_name=self.data_name,
            data_path=self.data_path,
        )

        return response

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

    def validate_results_path(self, path, format):
        """ensures path ends in the correct file format format"""
        p = os.path.splitext(path)[0]
        p = p + f".{format}"
        return p

    def save_responses(self, responses=None, results_path=None, format="csv"):
        if responses is None:
            responses = getattr(self, "responses", None)

        if not responses:
            return

        if results_path is None:
            results_path = self.results_file_path

        results_path = self.validate_results_path(results_path, format)
        fieldnames = [x for x in responses[0].__annotations__.keys() if x != "errors"]

        all_errors = list(
            chain.from_iterable([response.errors for response in responses])
        )

        def drop_errors(d):
            if "errors" in d.keys():
                del d["errors"]
            return d

        responses_as_dicts = map(
            drop_errors, [response.to_dict() for response in responses]
        )

        os.makedirs(os.path.dirname(results_path), exist_ok=True)
        with open(results_path, "w") as f:
            if format == "csv":
                dictwriter = DictWriter(f, fieldnames=fieldnames)
                dictwriter.writeheader()
                dictwriter.writerows(responses_as_dicts)
            elif format == "json":
                json.dump(responses_as_dicts, f)
            else:
                raise ValueError(f"format must be csv or json and cannot be {format}")

        errors_as_dicts = [error.to_dict() for error in all_errors]

        fieldnames = set(
            chain.from_iterable(
                [[keys for keys in dict.keys()] for dict in errors_as_dicts]
            )
        )

        with open(results_path + ".errors", "w") as f:
            if format == "csv":
                dictwriter = DictWriter(f, fieldnames=fieldnames)
                dictwriter.writeheader()
                dictwriter.writerows(errors_as_dicts)
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
