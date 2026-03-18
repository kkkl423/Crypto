import sqlite3
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
SQLITE_FILE = BASE_DIR / "SQLLite" / "fx_rates.sqlite"
TABLE_NAME = "fx_rates"


def read_fx_rates(sqlite_file: Path = SQLITE_FILE, table_name: str = TABLE_NAME) -> pd.DataFrame:
    with sqlite3.connect(sqlite_file) as connection:
        df = pd.read_sql_query(
            f"SELECT date, EURPLN, USDPLN, EURUSD FROM {table_name} ORDER BY date",
            connection,
        )

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df


if __name__ == "__main__":
    df = read_fx_rates()
    print(df.to_string(index=False))
