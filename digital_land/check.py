import duckdb
import pandas as pd


def duplicate_reference_check(issues=None, csv_path=None):
    # csv_path = "test.csv"
    duckdb.read_csv(csv_path)
    df = pd.read_csv(csv_path)

    print(df.head(5))
    return issues
