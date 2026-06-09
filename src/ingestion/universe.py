import pandas as pd
import requests
from io import StringIO

headers = {"User-Agent": "berhan.erik@gmail.com"}

sp500_url   = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
russell_url = "https://en.wikipedia.org/wiki/Russell_1000_Index"
tickers_url = "https://www.sec.gov/files/company_tickers.json"


def get_sp500_tickers() -> list[str]:
    response = requests.get(sp500_url, headers=headers, timeout=30)
    tables   = pd.read_html(StringIO(response.text))
    tickers  = tables[0]["Symbol"].tolist()
    print(f"S&P 500: {len(tickers)} şirket bulundu.")
    return tickers


def get_russell_tickers() -> list[str]:
    response = requests.get(russell_url, headers=headers, timeout=30)
    tables   = pd.read_html(StringIO(response.text))
    tickers  = tables[3]["Symbol"].tolist()
    print(f"Russell 1000: {len(tickers)} şirket bulundu.")
    return tickers


def get_sec_company_df() -> pd.DataFrame:
    response      = requests.get(tickers_url, headers=headers, timeout=30)
    df            = pd.DataFrame.from_dict(response.json(), orient="index")
    df["cik_str"] = df["cik_str"].astype(str).str.zfill(10)
    df["ticker"]  = df["ticker"].str.upper()
    return df


def get_sp500_sectors() -> pd.DataFrame:
    response = requests.get(sp500_url, headers=headers, timeout=30)
    tables   = pd.read_html(StringIO(response.text))
    df       = tables[0][["Symbol", "GICS Sector", "GICS Sub-Industry"]].copy()
    df.columns = ["ticker", "gics_sector", "gics_sub_industry"]
    df["ticker"] = df["ticker"].str.upper()
    return df


def get_universe() -> pd.DataFrame:
    print("SEC şirket listesi çekiliyor...")
    sec_df = get_sec_company_df()

    sp500   = get_sp500_tickers()
    russell = get_russell_tickers()

    combined = list(set(sp500 + russell))
    print(f"Birleştirilmiş universe: {len(combined)} şirket")

    matched = sec_df[sec_df["ticker"].isin(combined)].copy()
    matched = matched.reset_index(drop=True)
    print(f"SEC'te CIK eşleşen: {len(matched)} şirket")

    # Sector bilgisini ekle
    sectors = get_sp500_sectors()
    matched = matched.merge(sectors, on="ticker", how="left")
    matched["gics_sector"]       = matched["gics_sector"].fillna("Unknown")
    matched["gics_sub_industry"] = matched["gics_sub_industry"].fillna("Unknown")

    return matched


def get_universe_ciks() -> list[str]:
    return get_universe()["cik_str"].tolist()