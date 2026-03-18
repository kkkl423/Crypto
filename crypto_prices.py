from pathlib import Path
import time

import pandas as pd
import requests


BASE_DIR = Path(__file__).resolve().parent
EXCELS_DIR = BASE_DIR / "Excels"
OUTPUT_FILE = EXCELS_DIR / "binance_crypto_daily_prices.xlsx"
BASE_URL = "https://api.binance.com"
START_DATE = pd.Timestamp("2015-01-01")
END_DATE = pd.Timestamp.today().normalize()
INTERVAL = "1d"
LIMIT = 1000
ASSETS = ["BTC", "ETH", "SOL", "SUI", "XRP", "ADA", "LTC", "DOGE", "PEPE", "HYPE", "BNB"]
QUOTE_PRIORITY = ["USDT", "USDC"]


def get_binance_symbols() -> set[str]:
    response = requests.get(f"{BASE_URL}/api/v3/exchangeInfo", timeout=30)
    response.raise_for_status()
    info = response.json()
    return {item["symbol"] for item in info["symbols"]}


def fetch_daily_klines(symbol: str, start_time: pd.Timestamp, end_time: pd.Timestamp) -> pd.DataFrame:
    rows = []
    current_start = int(start_time.timestamp() * 1000)
    end_ms = int((end_time + pd.Timedelta(days=1) - pd.Timedelta(milliseconds=1)).timestamp() * 1000)

    while current_start <= end_ms:
        params = {
            "symbol": symbol,
            "interval": INTERVAL,
            "startTime": current_start,
            "endTime": end_ms,
            "limit": LIMIT,
        }

        response = requests.get(f"{BASE_URL}/api/v3/klines", params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if not data:
            break

        rows.extend(data)

        last_open_time = data[-1][0]
        current_start = last_open_time + 24 * 60 * 60 * 1000
        time.sleep(0.1)

    if not rows:
        return pd.DataFrame(columns=["date", symbol])

    df = pd.DataFrame(
        rows,
        columns=[
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_asset_volume",
            "number_of_trades",
            "taker_buy_base_volume",
            "taker_buy_quote_volume",
            "ignore",
        ],
    )

    df["date"] = pd.to_datetime(df["open_time"], unit="ms").dt.normalize()
    df[symbol] = pd.to_numeric(df["close"], errors="coerce")
    return df[["date", symbol]].drop_duplicates(subset=["date"]).reset_index(drop=True)


def resolve_symbol(asset: str, available_symbols: set[str]) -> str | None:
    for quote in QUOTE_PRIORITY:
        symbol = f"{asset}{quote}"
        if symbol in available_symbols:
            return symbol
    return None


def build_crypto_price_dataframe(
    assets: list[str] = ASSETS,
    start_date: pd.Timestamp = START_DATE,
    end_date: pd.Timestamp = END_DATE,
) -> pd.DataFrame:
    available_symbols = get_binance_symbols()
    full_index = pd.date_range(start=start_date, end=end_date, freq="D")
    prices = pd.DataFrame(index=full_index)
    prices.index.name = "date"

    for asset in assets:
        symbol = resolve_symbol(asset, available_symbols)

        if symbol is None:
            print(f"Fetching {asset}...")
            print(f"   -> no USDT/USDC pair available on Binance Spot, filling with NaN")
            prices[f"{asset}USD"] = pd.NA
            continue

        print(f"Fetching {symbol}...")
        asset_df = fetch_daily_klines(symbol, start_date, end_date).set_index("date")
        prices = prices.join(asset_df.rename(columns={symbol: f"{asset}USD"}), how="left")

    return prices.reset_index()


if __name__ == "__main__":
    df = build_crypto_price_dataframe()

    print(df.to_string(index=False))

    EXCELS_DIR.mkdir(parents=True, exist_ok=True)
    df.to_excel(OUTPUT_FILE, index=False)
