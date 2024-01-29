"""
Utility functions to support functions in the expectation module

notes:
- might want to remove QueryRunner at a future date as it loads spatialite which may
not be useful for everything
"""
import spatialite
import yaml
import pandas as pd


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
