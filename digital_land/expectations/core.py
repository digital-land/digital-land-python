import spatialite
import pandas as pd
import yaml
from pydantic.dataclasses import dataclass
from dataclasses_json import dataclass_json
from enum import Enum
from datetime import datetime
import warnings
import copy
import os
from typing import Optional


def transform_df_first_column_into_set(dataframe: pd.DataFrame) -> set:
    "Given a pd dataframe returns the first column as a python set"
    return set(dataframe.iloc[:, 0].unique())


def config_parser(filepath: str):
    "Will parse a config file"
    try:
        with open(filepath) as file:
            config = yaml.load(file, Loader=yaml.FullLoader)
            if config is not None:
                config = dict(config)
    except OSError:
        return None
    return config


class DataQualityException(Exception):
    """Exception raised for failed expectations with severity RaiseError.
    Attributes: response
    """

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class QueryRunner:
    "Class to run queries usings spatialite"

    def __init__(self, tested_dataset_path: str):
        "Receives a path/name of sqlite dataset against which it will run the queries"
        self.tested_dataset_path = tested_dataset_path

    def inform_dataset_path(self):
        return self.tested_dataset_path

    def run_query(self, sql_query: str, return_only_first_col_as_set: bool = False):
        """
        Receives a sql query and returns the results either in a pandas
        dataframe or just the first column as a set (this is useful to
        test presence or absence of items like tables, columns, etc).

        Note: connection is openned and closed at each query, but for use
        cases like the present one that would not offer big benefits and
        would mean having to dev thread-lcoal connection pools. For more
        info see: https://stackoverflow.com/a/14520670
        """
        with spatialite.connect(self.tested_dataset_path) as con:
            cursor = con.execute(sql_query)
            cols = [column[0] for column in cursor.description]
            results = pd.DataFrame.from_records(data=cursor.fetchall(), columns=cols)

        if return_only_first_col_as_set:
            return transform_df_first_column_into_set(results)
        else:
            return results


class SeverityEnum(str, Enum):
    """
    Enumeration for severity csv in specification, may need to be replaced in the future
    with a different mechanism to read from csv
    """

    critical = "critical"
    error = "error"
    warning = "warning"
    notice = "notice"
    info = "info"
    debug = "debug"


# TODO write unit tests for this class
@dataclass_json
@dataclass
class ExpectationResponse:
    """Class to keep inputs and results of expectations"""

    checkpoint: str = None
    result: bool = None
    severity: SeverityEnum = None
    msg: str = None
    details: Optional[dict] = None
    data_name: str = None
    data_path: str = None
    name: str = None
    description: Optional[str] = None
    expectation: str = None
    entry_date: Optional[str] = None
    tags: Optional[dict] = None

    def __post_init__(self):
        "Adds a few more interesting items and adjusts response for log"

        check_for_kwargs = self.expectation_input.get("kwargs", None)
        if check_for_kwargs:
            entry_date = check_for_kwargs.get("entry_date", None)
        else:
            entry_date = None

        if entry_date:
            self.entry_date = entry_date
        else:
            now = datetime.now()
            self.entry_date = now.isoformat()

    def save_to_file(self, dir_path: str):
        "Prepares a naming convention and saves the response to a provided path"

        if self.result:
            name_status = "success"
        else:
            name_status = "fail"

        name_hash = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")[:-3]
        file_name = f"{self.entry_date}_{name_status}_{self.expectation_input['expectation_name']}_{name_hash}.json"

        with open(os.path.join(dir_path, file_name), "w") as f:
            self_save_version = copy.deepcopy(self)
            self_save_version.result = str(self_save_version.result)
            f.write(self_save_version.to_json())

    def act_on_failure(self):
        """
        Returns 1 if severity is critical or 0 if severity is not critical
        raises a warning for failed tests
        Could be moved to expection suite class
        """

        failure_count = 0

        if not self.result:
            warnings.warn(self.msg)
            if self.severity == "critical":
                failure_count = 1

        return failure_count
