import requests
import pandas as pd

headers = {'User-Agent': "berhan.erik@gmail.com"}
tickers_url = "https://www.sec.gov/files/company_tickers.json"
submissions_url = "https://data.sec.gov/submissions"
facts_url = "https://data.sec.gov/api/xbrl/companyfacts"
concepts_url = "https://data.sec.gov/api/xbrl/companyconcept"

def get_company_tickers():
    response = requests.get(tickers_url, headers=headers, timeout=30)
    df = pd.DataFrame.from_dict(response.json(), orient='index')
    df["cik_str"] = df["cik_str"].astype(str).str.zfill(10)
    return df

def get_submissions(cik: str):
    response = requests.get(f'{submissions_url}/CIK{cik}.json', headers=headers, timeout=30)
    return pd.DataFrame.from_dict(response.json()['filings']['recent'])

def get_company_facts(cik: str):
    response = requests.get(f'{facts_url}/CIK{cik}.json', headers=headers, timeout=30)
    return pd.DataFrame.from_dict(response.json()['facts'])

def get_company_concept(cik: str, concept: str = "Assets", taxonomy: str = "us-gaap"):
    response = requests.get(
        f'{concepts_url}/CIK{cik}/{taxonomy}/{concept}.json',
        headers=headers,
        timeout=30
    )
    return pd.DataFrame.from_dict(response.json()['units']['USD'])