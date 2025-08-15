import pytest
import pandas as pd


def test_pandas_installed():
    try:
        import pandas
    except ImportError:
        pytest.fail("pandas is not installed")


def test_tables_exist():
    try:
        from query_duckdb import query_duckdb
        df = query_duckdb()
        assert isinstance(df, pd.DataFrame)

    except Exception as e:
        pytest.fail("query_duckdb failed to return a dataframe, error: {}".format(e))
