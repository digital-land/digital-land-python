import psycopg2
import pandas as pd
import urllib.parse as urlparse


def get_pg_connection(database_url, autocommit=False):
    url = urlparse.urlparse(database_url)
    database = url.path[1:]
    user = url.username
    password = url.password
    host = url.hostname
    port = url.port
    connection = psycopg2.connect(
        host=host, database=database, user=user, password=password, port=port
    )

    connection.autocommit = autocommit

    return connection


def get_df(table_name, conn):
    """
    helper function to get a dataframe from a table
    """
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table_name}")
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    df = pd.DataFrame(rows, columns=columns)
    cur.close()
    return df
