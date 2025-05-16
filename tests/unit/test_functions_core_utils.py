import pandas as pd
from unittest.mock import patch, Mock
from digital_land.utils.functions_core import datasette_query, query_sqlite


@patch("digital_land.utils.functions_core.sqlite3.connect")
def test_query_sqlite(mock_connect):
    mock_data = Mock()
    mock_data.description = [("organisation",), ("dataset",)]
    mock_data.fetchall.return_value = [("org1", "dataset1"), ("org2", "dataset2")]

    mock_con = Mock()
    mock_con.execute.return_value = mock_data
    mock_connect.return_value.__enter__.return_value = mock_con

    df = query_sqlite("db_path", "SELECT * FROM table")

    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["organisation", "dataset"]
    assert len(df) == 2
    assert df.iloc[0]["organisation"] == "org1"


@patch("digital_land.utils.functions_core.pd.read_csv")
def test_datasette_query(mock_read_csv):
    df_mock = pd.DataFrame({"organisation": ["org1", "org2"]})
    mock_read_csv.return_value = df_mock

    df = datasette_query("db", "SELECT organisation FROM table")
    assert isinstance(df, pd.DataFrame)
    assert "organisation" in df.columns
    assert df.equals(df_mock)
