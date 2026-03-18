import sqlite3
from pathlib import Path
import requests
import pandas as pd
from datetime import date, timedelta
from typing import List, Tuple, Optional

# ------------- Settings -------------
API_ROOT = "https://api.nbp.pl/api/exchangerates/rates"
USER_AGENT = "RE-Fuels-Analytics/1.0 (+contact: data@re-fuels.energy)"
EUR_TABLE = "A"
USD_TABLE = "A"
BASE_DIR = Path(__file__).resolve().parent
SQLITE_DIR = BASE_DIR / "SQLLite"
SQLITE_FILE = SQLITE_DIR / "fx_rates.sqlite"
SQLITE_TABLE = "fx_rates"

START_DATE = date(2015, 1, 1)
END_DATE = date.today()
CHUNK_DAYS = 93 

# ------------- Helpers -------------
def daterange_chunks(start: date, end: date, chunk_days: int = CHUNK_DAYS) -> List[Tuple[date, date]]:
    chunks = []
    cur = start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=chunk_days - 1), end)
        chunks.append((cur, chunk_end))
        cur = chunk_end + timedelta(days=1)
    return chunks

def fetch_currency_series(
    code: str,
    table: str,
    start: date,
    end: date,
    session: Optional[requests.Session] = None,
    timeout: float = 20.0,
) -> pd.Series:
    sess = session or requests.Session()
    headers = {"Accept": "application/json", "User-Agent": USER_AGENT}

    all_dates, all_values = [], []
    for a, b in daterange_chunks(start, end, CHUNK_DAYS):
        url = f"{API_ROOT}/{table}/{code}/{a.isoformat()}/{b.isoformat()}/?format=json"
        r = sess.get(url, headers=headers, timeout=timeout)
        if r.status_code == 200:
            for row in r.json().get("rates", []):
                all_dates.append(pd.to_datetime(row["effectiveDate"]))
                all_values.append(float(row["mid"]))  # PLN per 1 unit
        elif r.status_code in (400, 404):
            continue
        else:
            continue

    if not all_dates:
        return pd.Series(name=code, dtype="float64")

    s = pd.Series(all_values, index=pd.DatetimeIndex(all_dates), name=code).sort_index()
    s = s[~s.index.duplicated(keep="last")]
    return s

# ------------- Main build -------------
def build_fx_dataframe(start: date = START_DATE, end: date = END_DATE) -> pd.DataFrame:
    sess = requests.Session()
    sess.headers.update({"User-Agent": USER_AGENT})

    eur_pln = fetch_currency_series("EUR", EUR_TABLE, start, end, session=sess)
    usd_pln = fetch_currency_series("USD", USD_TABLE, start, end, session=sess)

    df_pln = pd.concat([eur_pln, usd_pln], axis=1).sort_index()

    out = pd.DataFrame(index=df_pln.index)
    if not df_pln.empty:
        out["EURPLN"] = df_pln["EUR"]
        out["USDPLN"] = df_pln["USD"]
        out["EURUSD"] = df_pln["EUR"] / df_pln["USD"]

        full_idx = pd.date_range(pd.Timestamp(start), pd.Timestamp(end), freq="D")
        out = out.reindex(full_idx).ffill()

    out = out.apply(pd.to_numeric, errors="coerce")
    out.index.name = "date"

    # Enforce start cutoff (keeps the forward-filled tail through `end`)
    out = out.loc[pd.Timestamp(start):pd.Timestamp(end)]
    return out


def build_eurusd_dataframe(start: date = START_DATE, end: date = END_DATE) -> pd.DataFrame:
    return build_fx_dataframe(start=start, end=end)


def build_eur_cross_dataframe(start: date = START_DATE, end: date = END_DATE) -> pd.DataFrame:
    return build_fx_dataframe(start=start, end=end)


def save_fx_to_sqlite(
    df: pd.DataFrame,
    sqlite_file: Path = SQLITE_FILE,
    table_name: str = SQLITE_TABLE,
) -> Path:
    sqlite_file.parent.mkdir(parents=True, exist_ok=True)

    sqlite_df = df.reset_index().copy()
    sqlite_df["date"] = sqlite_df["date"].dt.strftime("%Y-%m-%d")

    with sqlite3.connect(sqlite_file) as connection:
        sqlite_df.to_sql(table_name, connection, if_exists="replace", index=False)
        connection.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_date ON {table_name}(date)")
        connection.commit()

    return sqlite_file

if __name__ == "__main__":
    df_final = build_fx_dataframe()
    db_path = save_fx_to_sqlite(df_final)
    print(df_final)
    print(f"\nSaved SQLite DB to: {db_path}")
