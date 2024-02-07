import yaml
import warnings
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
        # self.data_name = Path(data_path).stem
        # self.expectation_suite_yaml = expectation_suite_yaml
        # self.query_runner = QueryRunner(self.data_path)

    def load():
        """filled in by child classes, ensures a config is loaded correctly should raise error if not"""
        pass

    def save(self, output_dir, format="csv"):
        self.save_responses(
            self.responses,
            os.path.join(output_dir,self.__class__.__name__+".csv"),
            format=format)

    def yaml_config_parser(self, filepath):
        "Will parse a config file"
        try:
            with open(filepath) as file:
                config = yaml.load(file, Loader=yaml.FullLoader)
                if config is not None:
                    config = dict(config)
                else:
                    warnings.warn("empty yaml file provided")

        except OSError:
            warnings.warn("no yaml file found")
            config = None
        return config

    def run_expectation(self, expectation_function, **kwargs):
        """
        runs a given function with the kwargs
        """
        #  = {**kwargs}
        # expectation_function = getattr(expectations, expectation[""])
        # TODO add an errors return detail below
        result, msg, details = expectation_function(
            # query_runner=self.query_runner,
            **kwargs,
        )
        if getattr(self, "responses", None):
            entry_date = self.entry_date
        else:
            now = datetime.now()
            entry_date = now.isoformat()
        arguements = {**kwargs}
        # TODO return errors and a response
        response = ExpectationResponse(
            entry_date=entry_date,
            name="Test Name", # arguements["name"],
            description="Test Description", # arguements.get("description", None),
            # TODO this won't work and should change it to function and get the name
            # of the function above
            expectation="Test expecation", # arguements["expectation"],
            severity="warning", # arguements["severity"],
            result=result,
            msg=msg,
            details={},
            data_name="None", # self.data_name,
            data_path="None", # self.data_path,
            expectation_input={**arguements},
            # TODO remove as not sure tags are neccessary tbh
            tags=arguements.get("tags", None),
        )

        return response

    # should be decided by the actualy checkpoint
    def run(self):
        #if not self.config:
        #    self.load_config()

        #if not self.config():
        #    warnings.warn("no configuration loaded so no expectations where ran")
        # self.expectation_suite_config = self.config_parser(self.expectation_suite_yaml)
        # if not self.expectation_suite_config:
        #     return

        self.responses = []
        # TODO do somewhere different but not sure how
        now = datetime.now()
        self.entry_date = now.isoformat()
        self.failed_expectation_with_error_severity = 0

        # self.expectations = self.expectation_suite_config.get("expectations", None)
        for expectation in self.expectations:
            response = self.run_expectation(expectation)
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

        if responses:
            if results_path is None:
                results_path = self.results_file_path

            results_path = self.validate_results_path(results_path, format)
            fieldnames = responses[0].__annotations__.keys()
            responses_as_dicts = [response.to_dict() for response in responses]

            os.makedirs(os.path.dirname(results_path), exist_ok=True)
            with open(results_path, "w") as f:
                if format == "csv":
                    dictwriter = DictWriter(f, fieldnames=fieldnames)
                    dictwriter.writeheader()
                    dictwriter.writerows(responses_as_dicts)
                elif format == "json":
                    json.dump(responses_as_dicts, f)
                else:
                    raise ValueError(
                        f"format must be csv or json and cannot be {format}"
                    )

    # feels not needed
    # def act_on_critical_error(self, failed_expectation_with_error_severity=None):
    #     if failed_expectation_with_error_severity is None:
    #         getattr(self, "failed_expectation_with_error_severity", None)

    #     if failed_expectation_with_error_severity:
    #         if failed_expectation_with_error_severity > 0:
    #             raise DataQualityException(
    #                 "One or more expectations with severity RaiseError failed, see results for more details"
    #             )
