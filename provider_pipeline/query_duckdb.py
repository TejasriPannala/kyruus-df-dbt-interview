import duckdb
import pandas as pd

def query_duckdb():
    con = duckdb.connect("dev.duckdb")
    df = con.execute("SELECT * FROM main.provider_address_agg").fetchdf()
    return df

if __name__ == "__main__":
    print(query_duckdb())