from pathlib import Path
import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
EXCELS_DIR = BASE_DIR / "Excels"
INPUT_FILE = EXCELS_DIR / "binance_crypto_transfers.xlsx"
OUTPUT_FILE = EXCELS_DIR / "binance_daily_crypto_balances.xlsx"
PRICE_FILE = EXCELS_DIR / "binance_crypto_daily_prices.xlsx"
TRACKED_COINS = ["BTC", "ETH", "SOL", "SUI", "XRP", "ADA", "LTC", "DOGE", "PEPE", "HYPE", "BNB", "USDT", "USDC"]


def get_tracked_coins(transfers: pd.DataFrame) -> list[str]:
    transfer_coins = sorted(set(transfers.get("coin", pd.Series(dtype="object")).dropna()))
    ordered = [coin for coin in TRACKED_COINS if coin in transfer_coins]
    extras = [coin for coin in transfer_coins if coin not in TRACKED_COINS]
    missing_defaults = [coin for coin in TRACKED_COINS if coin not in ordered]
    return ordered + extras + missing_defaults


def build_daily_crypto_balances() -> pd.DataFrame:
    transfers = pd.read_excel(INPUT_FILE).copy()

    if transfers.empty:
        return pd.DataFrame(columns=["date"] + TRACKED_COINS)

    transfers["insertTime"] = pd.to_datetime(transfers["insertTime"], errors="coerce")
    transfers["amount"] = pd.to_numeric(transfers["amount"], errors="coerce")

    valid_status_mask = (
        (transfers["type"].eq("deposit") & transfers["status"].eq(1))
        | (transfers["type"].eq("withdrawal") & transfers["status"].eq(6))
    )
    transfers = transfers.loc[valid_status_mask].copy()
    transfers = transfers.dropna(subset=["insertTime", "amount", "coin"])

    if transfers.empty:
        return pd.DataFrame(columns=["date"] + TRACKED_COINS)

    transfers["date"] = transfers["insertTime"].dt.normalize()
    transfers["signed_amount"] = transfers["amount"]
    transfers.loc[transfers["type"].eq("withdrawal"), "signed_amount"] *= -1

    coin_columns = get_tracked_coins(transfers)

    daily_flows = (
        transfers.pivot_table(
            index="date",
            columns="coin",
            values="signed_amount",
            aggfunc="sum",
            fill_value=0.0,
        )
        .sort_index()
    )

    daily_flows = daily_flows.reindex(columns=coin_columns, fill_value=0.0)

    full_index = pd.date_range(
        start=daily_flows.index.min(),
        end=pd.Timestamp.today().normalize(),
        freq="D",
    )

    daily_balances = daily_flows.reindex(full_index, fill_value=0.0).cumsum()
    daily_balances.index.name = "date"

    return daily_balances.reset_index()


def attach_daily_crypto_prices(daily_balances: pd.DataFrame) -> pd.DataFrame:
    if daily_balances.empty:
        return daily_balances

    prices = pd.read_excel(PRICE_FILE).copy()
    prices["date"] = pd.to_datetime(prices["date"], errors="coerce")

    merged = daily_balances.merge(prices, on="date", how="left")

    for stablecoin in ["USDT", "USDC"]:
        price_column = f"{stablecoin}USD"
        if price_column not in merged.columns:
            merged[price_column] = 1.0
        else:
            merged[price_column] = merged[price_column].fillna(1.0)

    coin_columns = [column for column in daily_balances.columns if column != "date"]
    for coin in coin_columns:
        source_price_column = f"{coin}USD"
        target_price_column = f"{coin}_price_usd"

        if source_price_column in merged.columns:
            merged[target_price_column] = merged[source_price_column]
            merged = merged.drop(columns=[source_price_column])
        else:
            merged[target_price_column] = pd.NA

    return merged


def build_daily_crypto_balances_with_prices() -> pd.DataFrame:
    daily_balances = build_daily_crypto_balances()
    return attach_daily_crypto_prices(daily_balances)


if __name__ == "__main__":
    df = build_daily_crypto_balances_with_prices()

    print(df.to_string(index=False))

    EXCELS_DIR.mkdir(parents=True, exist_ok=True)
    df.to_excel(OUTPUT_FILE, index=False)
