import spatialite
import pandas as pd
import yaml
from dataclasses import dataclass, field
from dataclasses_json import dataclass_json
from datetime import datetime
import warnings
import copy


def transform_df_first_column_into_set(dataframe: pd.DataFrame) -> set:
    "Given a pd dataframe returns the first column as a python set"
    return set(dataframe.iloc[:, 0].unique())


def config_parser(filepath: str):
    "Will parse a config file"
    with open(filepath) as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
        config = dict(config)
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


@dataclass_json
@dataclass
class ExpectationResponse:
    """Class to keep inputs and results of expectations"""

    expectation_input: dict
    result: bool = None
    msg: str = None
    details: dict = None
    sqlite_dataset: str = None
    data_quality_execution_time: str = field(init=False)

    def __post_init__(self):
        "Adds a few more interesting items and adjusts response for log"

        self.expectation_input.pop("query_runner")

        check_for_kwards = self.expectation_input.get("kwargs", None)
        if check_for_kwards:
            data_quality_execution_time = check_for_kwards.get(
                "data_quality_execution_time", None
            )
        else:
            data_quality_execution_time = None

        if data_quality_execution_time:
            self.data_quality_execution_time = data_quality_execution_time
        else:
            now = datetime.now()
            self.data_quality_execution_time = now.strftime("%Y%m%d_%H%M%S")

    def save_to_file(self, dir_path: str):
        "Prepares a naming convention and saves the response to a provided path"

        if self.result == True:
            name_status = "success"
        else:
            name_status = "fail"

        name_hash = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")[:-3]
        file_name = f"{self.data_quality_execution_time}_{name_status}_{self.expectation_input['expectation_name']}_{name_hash}.json"

        with open(dir_path + file_name, "w") as f:
            self_save_version = copy.deepcopy(self)
            self_save_version.result = str(self_save_version.result)
            f.write(self_save_version.to_json())

    def act_on_failure(self):
        "Raises error if severity is RaiseError or shows warning if severity is LogWarning"

        result = 0

        if self.result == False:
            warnings.warn(self.msg)
            if self.expectation_input["expectation_severity"] == "RaiseError":
                result = 1

        return result
