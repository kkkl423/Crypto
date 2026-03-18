import time
import hmac
import hashlib
from pathlib import Path
import requests
import pandas as pd
import keys as k
from datetime import datetime, timedelta, timezone

BASE_URL = "https://api.binance.com"
OUTPUT_DIR = Path("Excels")
OUTPUT_FILE = OUTPUT_DIR / "binance_crypto_transfers.xlsx"
START_DATE = datetime(2015, 1, 1, tzinfo=timezone.utc)
END_DATE = datetime.now(timezone.utc)
CHUNK_DAYS = 90
MAX_RETRIES = 6
BASE_BACKOFF_SECONDS = 2.0
OUTPUT_COLUMNS = [
    "type",
    "id",
    "amount",
    "coin",
    "network",
    "status",
    "address",
    "txId",
    "insertTime",
]


def signed_get(endpoint: str, params: dict, api_key: str, api_secret: str):
    query_string = "&".join(f"{key}={value}" for key, value in params.items())
    signature = hmac.new(
        api_secret.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

    headers = {
        "X-MBX-APIKEY": api_key
    }

    url = f"{BASE_URL}{endpoint}?{query_string}&signature={signature}"
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


def signed_get_with_retry(endpoint: str, params: dict, api_key: str, api_secret: str):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return signed_get(endpoint, params, api_key, api_secret)
        except requests.exceptions.HTTPError as exc:
            response = exc.response
            status_code = response.status_code if response is not None else None

            if status_code != 429 or attempt == MAX_RETRIES:
                raise

            retry_after = None
            if response is not None:
                retry_after_header = response.headers.get("Retry-After")
                if retry_after_header:
                    try:
                        retry_after = float(retry_after_header)
                    except ValueError:
                        retry_after = None

            wait_seconds = retry_after if retry_after is not None else BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
            print(f"   -> rate limited by Binance (429), retrying in {wait_seconds:.1f}s")
            time.sleep(wait_seconds)


def datetime_to_ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def chunk_ranges(start: datetime, end: datetime, chunk_days: int = CHUNK_DAYS):
    ranges = []
    current = start

    while current <= end:
        chunk_end = min(current + timedelta(days=chunk_days) - timedelta(milliseconds=1), end)
        ranges.append((current, chunk_end))
        current = chunk_end + timedelta(milliseconds=1)

    return ranges


def get_binance_fiat_deposits(start: datetime = START_DATE, end: datetime = END_DATE, rows: int = 100):
    all_rows = []

    for idx, (chunk_start, chunk_end) in enumerate(chunk_ranges(start, end), start=1):
        page = 1

        while True:
            print(f"[Chunk {idx} page {page}] {chunk_start.date()} -> {chunk_end.date()}")
            params = {
                "transactionType": 0,   # 0 = fiat deposit
                "beginTime": datetime_to_ms(chunk_start),
                "endTime": datetime_to_ms(chunk_end),
                "page": page,
                "rows": rows,
                "timestamp": int(time.time() * 1000),
            }

            response_json = signed_get_with_retry(
                endpoint="/sapi/v1/fiat/orders",
                params=params,
                api_key=k.API_KEY_BINANCE,
                api_secret=k.API_SECRET_BINANCE,
            )

            data = response_json.get("data", [])

            if not data:
                break

            all_rows.extend(data)
            print(f"   -> found {len(data)} fiat deposits")

            if len(data) < rows:
                break

            page += 1
            time.sleep(0.25)

        time.sleep(0.25)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)

    if "orderNo" in df.columns:
        df = df.drop_duplicates(subset=["orderNo"]).reset_index(drop=True)

    for col in ["createTime", "updateTime"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], unit="ms", errors="coerce")

    return df


def get_binance_crypto_deposits(start: datetime = START_DATE, end: datetime = END_DATE):
    all_rows = []

    for idx, (chunk_start, chunk_end) in enumerate(chunk_ranges(start, end), start=1):
        print(f"[Crypto Chunk {idx}] {chunk_start.date()} -> {chunk_end.date()}")
        params = {
            "startTime": datetime_to_ms(chunk_start),
            "endTime": datetime_to_ms(chunk_end),
            "timestamp": int(time.time() * 1000),
        }

        response_json = signed_get_with_retry(
            endpoint="/sapi/v1/capital/deposit/hisrec",
            params=params,
            api_key=k.API_KEY_BINANCE,
            api_secret=k.API_SECRET_BINANCE,
        )

        if isinstance(response_json, list):
            all_rows.extend(response_json)
            print(f"   -> found {len(response_json)} crypto deposits")

        time.sleep(0.25)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)

    if "id" in df.columns:
        df = df.drop_duplicates(subset=["id"]).reset_index(drop=True)

    df["type"] = "deposit"

    for col in ["insertTime", "completeTime"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], unit="ms", errors="coerce")

    df = df.reindex(columns=OUTPUT_COLUMNS)
    return df


def get_binance_crypto_withdrawals(start: datetime = START_DATE, end: datetime = END_DATE):
    all_rows = []

    for idx, (chunk_start, chunk_end) in enumerate(chunk_ranges(start, end), start=1):
        print(f"[Withdrawal Chunk {idx}] {chunk_start.date()} -> {chunk_end.date()}")
        params = {
            "startTime": datetime_to_ms(chunk_start),
            "endTime": datetime_to_ms(chunk_end),
            "timestamp": int(time.time() * 1000),
        }

        response_json = signed_get_with_retry(
            endpoint="/sapi/v1/capital/withdraw/history",
            params=params,
            api_key=k.API_KEY_BINANCE,
            api_secret=k.API_SECRET_BINANCE,
        )

        if isinstance(response_json, list):
            all_rows.extend(response_json)
            print(f"   -> found {len(response_json)} crypto withdrawals")

        time.sleep(0.25)

    if not all_rows:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    df = pd.DataFrame(all_rows)

    if "id" in df.columns:
        df = df.drop_duplicates(subset=["id"]).reset_index(drop=True)

    df["type"] = "withdrawal"

    if "applyTime" in df.columns:
        df["insertTime"] = pd.to_datetime(df["applyTime"], errors="coerce")
    elif "completeTime" in df.columns:
        df["insertTime"] = pd.to_datetime(df["completeTime"], errors="coerce")
    else:
        df["insertTime"] = pd.NaT

    df = df.reindex(columns=OUTPUT_COLUMNS)
    return df


def get_binance_crypto_transfers(start: datetime = START_DATE, end: datetime = END_DATE):
    deposits = get_binance_crypto_deposits(start=start, end=end)
    withdrawals = get_binance_crypto_withdrawals(start=start, end=end)
    combined = pd.concat([deposits, withdrawals], ignore_index=True)

    if combined.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    combined = combined.sort_values("insertTime", na_position="last").reset_index(drop=True)
    return combined


if __name__ == "__main__":
    df = get_binance_crypto_transfers()

    print("Columns:")
    print(df.columns.tolist())
    print()
    print(df)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_excel(OUTPUT_FILE, index=False)
