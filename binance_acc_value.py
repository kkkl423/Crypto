from pathlib import Path

import pandas as pd

from binance_fiat_depo_FX import enrich_fiat_operations_with_fx
from binance_crypto_transfers import build_daily_crypto_balances


BASE_DIR = Path(__file__).resolve().parent
EXCELS_DIR = BASE_DIR / "Excels"
OUTPUT_FILE = EXCELS_DIR / "binance_fiat_daily_account_value.xlsx"


def build_daily_fiat_account_value() -> pd.DataFrame:
    fiat = enrich_fiat_operations_with_fx().copy()

    fiat = fiat.dropna(subset=["Date(UTC+1)", "Receive Amount USD", "Receive Amount EUR", "Type"])
    fiat["date"] = fiat["Date(UTC+1)"].dt.normalize()

    deposit_mask = fiat["Type"].eq("Deposit")
    withdrawal_mask = fiat["Type"].eq("Withdrawal")

    fiat["net_flow_usd"] = 0.0
    fiat["net_flow_eur"] = 0.0
    fiat.loc[deposit_mask, "net_flow_usd"] = fiat.loc[deposit_mask, "Receive Amount USD"]
    fiat.loc[withdrawal_mask, "net_flow_usd"] = -fiat.loc[withdrawal_mask, "Receive Amount USD"]
    fiat.loc[deposit_mask, "net_flow_eur"] = fiat.loc[deposit_mask, "Receive Amount EUR"]
    fiat.loc[withdrawal_mask, "net_flow_eur"] = -fiat.loc[withdrawal_mask, "Receive Amount EUR"]

    daily_flows = (
        fiat.groupby("date", as_index=False)[["net_flow_usd", "net_flow_eur"]]
        .sum()
        .sort_values("date")
        .reset_index(drop=True)
    )

    if daily_flows.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "daily_net_flow_usd",
                "daily_net_flow_eur",
                "account_value_usd",
                "acc_value_eur",
            ]
        )

    full_index = pd.date_range(
        start=daily_flows["date"].min(),
        end=pd.Timestamp.today().normalize(),
        freq="D",
    )

    daily_values = (
        daily_flows.set_index("date")
        .reindex(full_index, fill_value=0.0)
        .rename_axis("date")
        .reset_index()
    )

    daily_values = daily_values.rename(
        columns={
            "net_flow_usd": "daily_net_flow_usd",
            "net_flow_eur": "daily_net_flow_eur",
        }
    )
    daily_values["account_value_usd"] = daily_values["daily_net_flow_usd"].cumsum()
    daily_values["acc_value_eur"] = daily_values["daily_net_flow_eur"].cumsum()

    crypto_balances = build_daily_crypto_balances()
    if crypto_balances.empty:
        return daily_values

    combined = daily_values.merge(crypto_balances, on="date", how="outer").sort_values("date").reset_index(drop=True)

    base_columns = [
        "daily_net_flow_usd",
        "daily_net_flow_eur",
        "account_value_usd",
        "acc_value_eur",
    ]
    crypto_columns = [column for column in combined.columns if column not in ["date", *base_columns]]

    combined[base_columns] = combined[base_columns].ffill().fillna(0.0)
    combined[["daily_net_flow_usd", "daily_net_flow_eur"]] = combined[["daily_net_flow_usd", "daily_net_flow_eur"]].fillna(0.0)
    combined[crypto_columns] = combined[crypto_columns].ffill().fillna(0.0)

    return combined


if __name__ == "__main__":
    df = build_daily_fiat_account_value()

    print(df.to_string(index=False))

    EXCELS_DIR.mkdir(parents=True, exist_ok=True)
    df.to_excel(OUTPUT_FILE, index=False)
