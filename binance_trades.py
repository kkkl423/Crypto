import time
import pandas as pd
import keys as k
from binance.spot import Spot
from exchange_rates import build_eurusd_dataframe

client = Spot(api_key=k.API_KEY_BINANCE, api_secret=k.API_SECRET_BINANCE)
QUOTE_ASSETS = ["USDT", "USDC", "EURC", "USD", "EUR"]
EUR_QUOTES = {"EUR", "EURC"}
USD_QUOTES = {"USD", "USDT", "USDC"}
USD_LIKE_ASSETS = {"USD", "USDT", "USDC"}
EUR_LIKE_ASSETS = {"EUR", "EURC"}

OUTPUT_COLUMNS = [
    "symbol",
    "id",
    "orderId",
    "price",
    "qty",
    "quoteQty",
    "commission",
    "commissionAsset",
    "time",
    "side",
    "EURUSD",
    "USD Value",
    "USD Commission",
]


def attach_eurusd_fx(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "time" not in df.columns:
        return df

    trade_dates = df["time"].dropna()
    if trade_dates.empty:
        df["EURUSD"] = pd.NA
        return df

    fx = build_eurusd_dataframe(
        start=trade_dates.min().date(),
        end=trade_dates.max().date(),
    )

    fx_by_date = fx["EURUSD"]
    enriched = df.copy()
    enriched["EURUSD"] = enriched["time"].dt.normalize().map(fx_by_date)
    return enriched


def attach_usd_value(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    enriched = df.copy()
    enriched["USD Value"] = pd.NA

    if not {"symbol", "price", "qty"}.issubset(enriched.columns):
        return enriched

    quote_asset = enriched["symbol"].apply(extract_quote_asset)
    gross_value = enriched["price"] * enriched["qty"]

    eur_mask = quote_asset.isin(EUR_QUOTES)
    usd_mask = quote_asset.isin(USD_QUOTES)

    enriched.loc[eur_mask, "USD Value"] = gross_value[eur_mask] * enriched.loc[eur_mask, "EURUSD"]
    enriched.loc[usd_mask, "USD Value"] = gross_value[usd_mask]

    enriched["USD Value"] = pd.to_numeric(enriched["USD Value"], errors="coerce")
    return enriched


def extract_quote_asset(symbol: str):
    if not isinstance(symbol, str):
        return pd.NA

    for quote in QUOTE_ASSETS:
        if symbol.endswith(quote):
            return quote

    return pd.NA


def extract_base_asset(symbol: str):
    quote_asset = extract_quote_asset(symbol)
    if not isinstance(symbol, str) or pd.isna(quote_asset):
        return pd.NA

    return symbol[: -len(quote_asset)]


def attach_usd_commission(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    enriched = df.copy()
    enriched["USD Commission"] = pd.NA

    required_columns = {"symbol", "price", "commission", "commissionAsset", "EURUSD"}
    if not required_columns.issubset(enriched.columns):
        return enriched

    quote_asset = enriched["symbol"].apply(extract_quote_asset)
    base_asset = enriched["symbol"].apply(extract_base_asset)

    commission_asset = enriched["commissionAsset"]
    commission_amount = enriched["commission"]

    base_mask = commission_asset.eq(base_asset)
    quote_mask = commission_asset.eq(quote_asset)
    eur_mask = commission_asset.isin(EUR_LIKE_ASSETS)
    usd_mask = commission_asset.isin(USD_LIKE_ASSETS)

    quote_value = commission_amount.copy()
    quote_value.loc[base_mask] = commission_amount[base_mask] * enriched.loc[base_mask, "price"]

    value_in_quote_eur_mask = (base_mask | quote_mask) & quote_asset.isin(EUR_QUOTES)
    value_in_quote_usd_mask = (base_mask | quote_mask) & quote_asset.isin(USD_QUOTES)

    enriched.loc[value_in_quote_eur_mask, "USD Commission"] = (
        quote_value[value_in_quote_eur_mask] * enriched.loc[value_in_quote_eur_mask, "EURUSD"]
    )
    enriched.loc[value_in_quote_usd_mask, "USD Commission"] = quote_value[value_in_quote_usd_mask]
    enriched.loc[eur_mask, "USD Commission"] = commission_amount[eur_mask] * enriched.loc[eur_mask, "EURUSD"]
    enriched.loc[usd_mask, "USD Commission"] = commission_amount[usd_mask]

    enriched["USD Commission"] = pd.to_numeric(enriched["USD Commission"], errors="coerce")
    return enriched


def get_selected_binance_spot_trades():
    bases = ["BTC", "ETH", "SOL", "SUI", "DOGE", "BNB", "XRP", "ADA", "PEPE", "LTC"]

    quotes = QUOTE_ASSETS

    symbols = [f"{base}{quote}" for base in bases for quote in quotes]

    all_trades = []
    symbols_with_trades = []

    for i, symbol in enumerate(symbols, start=1):
        print(f"[{i}/{len(symbols)}] Checking {symbol}...")
        from_id = None
        symbol_trade_count = 0

        while True:
            try:
                params = {"symbol": symbol, "limit": 1000}
                if from_id is not None:
                    params["fromId"] = from_id

                trades = client.my_trades(**params)
            except Exception:
                break

            if not trades:
                break

            symbol_trade_count += len(trades)

            for t in trades:
                t["symbol"] = symbol
                all_trades.append(t)

            if len(trades) < 1000:
                break

            from_id = trades[-1]["id"] + 1
            time.sleep(0.05)

        if symbol_trade_count > 0:
            symbols_with_trades.append(symbol)
            print(f"   -> found {symbol_trade_count} trades")

    print("\nSymbols with trades:")
    print(symbols_with_trades)

    if not all_trades:
        return pd.DataFrame()

    df = pd.DataFrame(all_trades)

    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], unit="ms")

    for col in ["price", "qty", "quoteQty", "commission"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "isBuyer" in df.columns:
        df["side"] = df["isBuyer"].map({True: "BUY", False: "SELL"})

    df = df.sort_values("time").reset_index(drop=True)
    df = attach_eurusd_fx(df)
    df = attach_usd_value(df)
    df = attach_usd_commission(df)
    df = df.reindex(columns=OUTPUT_COLUMNS)

    print("\nAVAILABLE COLUMNS:")
    print(df.columns.tolist())

    return df


if __name__ == "__main__":
    df = get_selected_binance_spot_trades()

    print("\nFULL DATAFRAME:")
    print(df)

    print(f"\nTotal trades found: {len(df)}")

    df.to_excel("binance_selected_spot_trades.xlsx", index=False)
