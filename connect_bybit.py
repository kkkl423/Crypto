import keys as k
from pybit.unified_trading import HTTP

session = HTTP(
    testnet=False,
    api_key=k.API_KEY_BYBIT,
    api_secret=k.API_SECRET_BYBIT,
)

result = session.get_wallet_balance(accountType="UNIFIED")

account = result["result"]["list"][0]

print("Total Equity:", account["totalEquity"], "USD")