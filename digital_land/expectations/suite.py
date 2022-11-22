import yaml
import warnings
from datetime import datetime
from pathlib import Path
from csv import DictWriter
import logging
import os

from digital_land.expectations.core import QueryRunner, DataQualityException, ExpectationResponse
import digital_land.expectations.expectations as expectations


class DatasetExpectationSuite:
    def __init__(self,results_file_path, data_path, expectation_suite_yaml):
        self.results_file_path = results_file_path
        self.data_path = data_path
        self.data_name = Path(data_path).stem
        self.expectation_suite_yaml = expectation_suite_yaml
        self.query_runner = QueryRunner(self.data_path)

    def config_parser(self,filepath):
        "Will parse a config file"
        try:
            with open(filepath) as file:
                config = yaml.load(file, Loader=yaml.FullLoader)
                if config is not None:
                    config = dict(config)
                else:
                    warnings.warn('empty yaml file provided')

        except OSError:
            warnings.warn('no yaml file found')
            config= None
        return config

    def run_expectation(self,expectation):
        arguments = {**expectation}
        expectation_function=getattr(expectations,expectation['expectation']) 
        result, msg, details = expectation_function(
            query_runner=self.query_runner,
            **arguments,
        )
        if getattr(self,'responses',None):
            entry_date =  self.entry_date
        else:
            now = datetime.now()
            entry_date = now.isoformat()

        response = ExpectationResponse(
            entry_date=entry_date,
            name=expectation['name'],
            description=expectation.get('description',None),
            expectation=expectation['expectation'],
            severity=expectation['severity'],
            result=result,
            msg=msg,
            details=details,
            data_name=self.data_name,
            data_path=self.data_path,
            expectation_input={**expectation},
        )

        return response
    
    def run_suite(self):
        self.expectation_suite_config=self.config_parser(self.expectation_suite_yaml)
        if not self.expectation_suite_config:
            return

        self.responses=[]
        now = datetime.now()
        self.entry_date = now.isoformat()
        self.failed_expectation_with_error_severity = 0
        
        self.expectations = self.expectation_suite_config.get("expectations", None)
        for expectation in self.expectations:
            response = self.run_expectation(expectation)
            self.responses.append(response)
            self.failed_expectation_with_error_severity += response.act_on_failure()
    
        if self.failed_expectation_with_error_severity > 0:
            raise DataQualityException(
                "One or more expectations with severity RaiseError failed, see results for more details"
            )
    
    def save_responses(self,responses=None,results_path=None):
        if responses is None:
            responses=getattr(self,'responses',None)
        
        if responses:
            if results_path == None:
                results_path=self.results_file_path
            fieldnames=responses[0].__annotations__.keys()
            responses_as_dicts = [response.to_dict() for response in responses]

            os.makedirs(os.path.dirname(results_path), exist_ok=True)
            with open(results_path, "w") as f:
                dictwriter = DictWriter(f, fieldnames=fieldnames)
                dictwriter.writeheader()
                dictwriter.writerows(responses_as_dicts)
    
    def act_on_critical_error(self,failed_expectation_with_error_severity=None):
        if failed_expectation_with_error_severity is None:
           getattr(self,'failed_expectation_with_error_severity',None)

        if failed_expectation_with_error_severity:
            if failed_expectation_with_error_severity > 0:
                raise DataQualityException(
                    "One or more expectations with severity RaiseError failed, see results for more details"
                )
