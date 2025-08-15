import pytest

def test_pandas_installed():
    try:
        import pandas
    except ImportError:
        pytest.fail("pandas is not installed")
