import urllib
import sqlite3
import pandas as pd


def query_sqlite(db_path, query_string):
    with sqlite3.connect(db_path) as con:
        cursor = con.execute(query_string)
        cols = [column[0] for column in cursor.description]
        results_df = pd.DataFrame.from_records(data=cursor.fetchall(), columns=cols)
    return results_df


def datasette_query(db, sql_string):
    params = urllib.parse.urlencode({"sql": sql_string, "_size": "max"})
    url = f"https://datasette.planning.data.gov.uk/{db}.csv?{params}"
    df = pd.read_csv(url)
    return df
