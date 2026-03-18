import keys as k
import krakenex

api = krakenex.API()
api.key = k.API_KEY_KRAKEN
api.secret = k.API_SECRET_KRAKEN

def clean_asset(asset):
    mapping = {
        "XETH": "ETH",
        "XXBT": "BTC",
        "XXRP": "XRP",
        "XXDG": "DOGE",
        "ZEUR": "EUR",
        "ZUSD": "USD",
    }
    return mapping.get(asset, asset)

def get_price_usd(public_api, asset):
    # Assets already in USD terms
    if asset in ["USD", "ZUSD"]:
        return 1.0

    # Stablecoins close to USD
    if asset in ["USDT", "USDC"]:
        return 1.0

    # EUR -> USD
    if asset in ["EUR", "ZEUR"]:
        r = public_api.query_public("Ticker", {"pair": "EURUSD"})
        if not r["error"]:
            pair_data = next(iter(r["result"].values()))
            return float(pair_data["c"][0])

    # Crypto -> USD direct
    candidates = [
        f"{asset}USD",
        f"{asset}ZUSD",
        f"X{asset}ZUSD",
    ]

    for pair in candidates:
        r = public_api.query_public("Ticker", {"pair": pair})
        if not r["error"] and r["result"]:
            pair_data = next(iter(r["result"].values()))
            return float(pair_data["c"][0])

    # Fallback for ETH and BTC Kraken codes
    special_pairs = {
        "ETH": "XETHZUSD",
        "XETH": "XETHZUSD",
        "BTC": "XXBTZUSD",
        "XXBT": "XXBTZUSD",
        "XRP": "XXRPZUSD",
        "XXRP": "XXRPZUSD",
        "DOGE": "XDGUSD",
        "XXDG": "XDGUSD",
    }

    if asset in special_pairs:
        r = public_api.query_public("Ticker", {"pair": special_pairs[asset]})
        if not r["error"] and r["result"]:
            pair_data = next(iter(r["result"].values()))
            return float(pair_data["c"][0])

    return 0.0


result = api.query_private("Balance")

if result["error"]:
    print("Error:", result["error"])
else:
    balances = result["result"]
    total_usd = 0.0

    print("Non-zero balances in USD:")
    for asset, amount in balances.items():
        qty = float(amount)

        if qty > 0:
            px_usd = get_price_usd(api, asset)
            value_usd = qty * px_usd
            total_usd += value_usd

            print(f"{clean_asset(asset):>6} | qty = {qty:<14.8f} | value = {value_usd:>10.2f} USD")

    print(f"\nTOTAL ACCOUNT VALUE = {total_usd:.2f} USD")