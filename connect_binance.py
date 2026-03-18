import keys as k
from binance.spot import Spot

client = Spot(api_key=k.API_KEY_BINANCE, api_secret=k.API_SECRET_BINANCE)

STABLES_1USD = {"USDT", "USDC", "BUSD", "FDUSD"}

def safe_price(symbol):
    try:
        return float(client.ticker_price(symbol)["price"])
    except:
        return None

def get_price_in_usdt(asset):
    if asset in STABLES_1USD:
        return 1.0

    # direct pairs
    for quote in ["USDT", "USDC"]:
        px = safe_price(f"{asset}{quote}")
        if px is not None:
            return px

    # common cross conversions
    btc_usdt = safe_price("BTCUSDT")
    eth_usdt = safe_price("ETHUSDT")
    bnb_usdt = safe_price("BNBUSDT")

    asset_btc = safe_price(f"{asset}BTC")
    if asset_btc is not None and btc_usdt is not None:
        return asset_btc * btc_usdt

    asset_eth = safe_price(f"{asset}ETH")
    if asset_eth is not None and eth_usdt is not None:
        return asset_eth * eth_usdt

    asset_bnb = safe_price(f"{asset}BNB")
    if asset_bnb is not None and bnb_usdt is not None:
        return asset_bnb * bnb_usdt

    # fiat fallback examples
    usdt_eur = safe_price("EURUSDT")
    if asset == "EUR" and usdt_eur is not None:
        return usdt_eur

    return 0.0


# -------------------
# 1) SPOT
# -------------------
spot_info = client.account()
spot_total_usdt = 0.0

print("\n=== SPOT ===")
for b in spot_info["balances"]:
    qty = float(b["free"]) + float(b["locked"])
    asset = b["asset"]

    if qty > 0:
        px = get_price_in_usdt(asset)
        value = qty * px
        spot_total_usdt += value
        print(f"{asset:>8} | qty = {qty:<14.8f} | value = {value:>12.2f} USDT")


# -------------------
# 2) CROSS MARGIN
# -------------------
cross_total_usdt = 0.0
btc_usdt = safe_price("BTCUSDT") or 0.0

print("\n=== CROSS MARGIN ===")
try:
    cross = client.margin_account()

    # Best case: Binance gives total net BTC value directly
    if "totalNetAssetOfBtc" in cross:
        cross_total_btc = float(cross["totalNetAssetOfBtc"])
        cross_total_usdt = cross_total_btc * btc_usdt
        print(f"Cross margin net asset = {cross_total_btc:.8f} BTC = {cross_total_usdt:.2f} USDT")

    else:
        # fallback: sum userAssets netAsset if available
        for a in cross.get("userAssets", []):
            net_asset = float(a.get("netAsset", 0))
            asset = a["asset"]

            if net_asset != 0:
                px = get_price_in_usdt(asset)
                value = net_asset * px
                cross_total_usdt += value
                print(f"{asset:>8} | net = {net_asset:<14.8f} | value = {value:>12.2f} USDT")

        print(f"Cross margin total = {cross_total_usdt:.2f} USDT")

except Exception as e:
    print("Could not read cross margin:", e)


# -------------------
# 3) ISOLATED MARGIN
# -------------------
isolated_total_usdt = 0.0

print("\n=== ISOLATED MARGIN ===")
try:
    isolated = client.isolated_margin_account()

    # Some responses include total net BTC directly
    if "totalNetAssetOfBtc" in isolated:
        isolated_total_btc = float(isolated["totalNetAssetOfBtc"])
        isolated_total_usdt = isolated_total_btc * btc_usdt
        print(f"Isolated margin total net asset = {isolated_total_btc:.8f} BTC = {isolated_total_usdt:.2f} USDT")

    else:
        # fallback: per isolated pair
        for pair in isolated.get("assets", []):
            symbol = pair.get("symbol", "UNKNOWN")

            base = pair.get("baseAsset", {})
            quote = pair.get("quoteAsset", {})

            base_net = float(base.get("netAsset", 0))
            quote_net = float(quote.get("netAsset", 0))

            base_asset = base.get("asset")
            quote_asset = quote.get("asset")

            base_val = base_net * get_price_in_usdt(base_asset) if base_asset else 0.0
            quote_val = quote_net * get_price_in_usdt(quote_asset) if quote_asset else 0.0

            pair_total = base_val + quote_val
            isolated_total_usdt += pair_total

            if pair_total != 0:
                print(f"{symbol:>12} | value = {pair_total:>12.2f} USDT")

        print(f"Isolated margin total = {isolated_total_usdt:.2f} USDT")

except Exception as e:
    print("Could not read isolated margin:", e)


# -------------------
# FINAL TOTAL
# -------------------
grand_total = spot_total_usdt + cross_total_usdt + isolated_total_usdt

print("\n==============================")
print(f"SPOT TOTAL           : {spot_total_usdt:12.2f} USDT")
print(f"CROSS MARGIN TOTAL   : {cross_total_usdt:12.2f} USDT")
print(f"ISOLATED MARGIN TOTAL: {isolated_total_usdt:12.2f} USDT")
print("------------------------------")
print(f"GRAND TOTAL          : {grand_total:12.2f} USDT")
print("==============================")