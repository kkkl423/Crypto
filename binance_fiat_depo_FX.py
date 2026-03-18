from pathlib import Path
import pandas as pd

from exchange_rates import build_fx_dataframe


BASE_DIR = Path(__file__).resolve().parent
EXCELS_DIR = BASE_DIR / "Excels"
FIAT_FILE = EXCELS_DIR / "Fiat_Operations.xlsx"
OUTPUT_FILE = EXCELS_DIR / "Fiat_Operations_with_FX.xlsx"


def split_amount_and_currency(series: pd.Series) -> pd.DataFrame:
    extracted = series.astype(str).str.extract(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([A-Z]+)\s*$")
    return pd.DataFrame(
        {
            "amount": pd.to_numeric(extracted[0], errors="coerce"),
            "currency": extracted[1],
        }
    )


def convert_to_usd(
    amount: pd.Series,
    currency: pd.Series,
    eurusd: pd.Series,
    usdpln: pd.Series,
) -> pd.Series:
    usd_value = pd.Series(pd.NA, index=amount.index, dtype="object")

    eur_mask = currency.eq("EUR")
    usd_mask = currency.eq("USD")
    pln_mask = currency.eq("PLN")

    usd_value.loc[eur_mask] = amount[eur_mask] * eurusd[eur_mask]
    usd_value.loc[usd_mask] = amount[usd_mask]
    usd_value.loc[pln_mask] = amount[pln_mask] / usdpln[pln_mask]

    return pd.to_numeric(usd_value, errors="coerce")


def convert_to_eur(
    amount: pd.Series,
    currency: pd.Series,
    eurusd: pd.Series,
    eurpln: pd.Series,
) -> pd.Series:
    eur_value = pd.Series(pd.NA, index=amount.index, dtype="object")

    eur_mask = currency.eq("EUR")
    usd_mask = currency.eq("USD")
    pln_mask = currency.eq("PLN")

    eur_value.loc[eur_mask] = amount[eur_mask]
    eur_value.loc[usd_mask] = amount[usd_mask] / eurusd[usd_mask]
    eur_value.loc[pln_mask] = amount[pln_mask] / eurpln[pln_mask]

    return pd.to_numeric(eur_value, errors="coerce")


def enrich_fiat_operations_with_fx() -> pd.DataFrame:
    fiat = pd.read_excel(FIAT_FILE).copy()

    fiat["Date(UTC+1)"] = pd.to_datetime(fiat["Date(UTC+1)"], errors="coerce")

    deposit_parts = split_amount_and_currency(fiat["Deposit Amount"])
    receive_parts = split_amount_and_currency(fiat["Receive Amount"])
    fee_parts = split_amount_and_currency(fiat["Fee"])

    fiat["Deposit Amount Value"] = deposit_parts["amount"]
    fiat["Deposit Amount Currency"] = deposit_parts["currency"]
    fiat["Receive Amount Value"] = receive_parts["amount"]
    fiat["Receive Amount Currency"] = receive_parts["currency"]
    fiat["Fee Value"] = fee_parts["amount"]
    fiat["Fee Currency"] = fee_parts["currency"]

    valid_dates = fiat["Date(UTC+1)"].dropna()
    if valid_dates.empty:
        fiat["EURUSD"] = pd.NA
        fiat["EURPLN"] = pd.NA
        fiat["USDPLN"] = pd.NA
    else:
        fx = build_fx_dataframe(
            start=valid_dates.min().date(),
            end=valid_dates.max().date(),
        )
        fiat["EURUSD"] = fiat["Date(UTC+1)"].dt.normalize().map(fx["EURUSD"])
        fiat["EURPLN"] = fiat["Date(UTC+1)"].dt.normalize().map(fx["EURPLN"])
        fiat["USDPLN"] = fiat["Date(UTC+1)"].dt.normalize().map(fx["USDPLN"])

    fiat["Deposit Amount USD"] = convert_to_usd(
        fiat["Deposit Amount Value"],
        fiat["Deposit Amount Currency"],
        fiat["EURUSD"],
        fiat["USDPLN"],
    )
    fiat["Receive Amount USD"] = convert_to_usd(
        fiat["Receive Amount Value"],
        fiat["Receive Amount Currency"],
        fiat["EURUSD"],
        fiat["USDPLN"],
    )
    fiat["Fee USD"] = convert_to_usd(
        fiat["Fee Value"],
        fiat["Fee Currency"],
        fiat["EURUSD"],
        fiat["USDPLN"],
    )
    fiat["Deposit Amount EUR"] = convert_to_eur(
        fiat["Deposit Amount Value"],
        fiat["Deposit Amount Currency"],
        fiat["EURUSD"],
        fiat["EURPLN"],
    )
    fiat["Receive Amount EUR"] = convert_to_eur(
        fiat["Receive Amount Value"],
        fiat["Receive Amount Currency"],
        fiat["EURUSD"],
        fiat["EURPLN"],
    )
    fiat["Fee EUR"] = convert_to_eur(
        fiat["Fee Value"],
        fiat["Fee Currency"],
        fiat["EURUSD"],
        fiat["EURPLN"],
    )

    return fiat.sort_values("Date(UTC+1)").reset_index(drop=True)


if __name__ == "__main__":
    df = enrich_fiat_operations_with_fx()

    print(df)

    EXCELS_DIR.mkdir(parents=True, exist_ok=True)
    df.to_excel(OUTPUT_FILE, index=False)
