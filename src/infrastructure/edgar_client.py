import requests
import pandas as pd
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

headers = {"User-Agent": "berhan.erik@gmail.com"}

tickers_url     = "https://www.sec.gov/files/company_tickers.json"
submissions_url = "https://data.sec.gov/submissions"
facts_url       = "https://data.sec.gov/api/xbrl/companyfacts"
concepts_url    = "https://data.sec.gov/api/xbrl/companyconcept"

session = requests.Session()
session.headers.update(headers)
retries = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504]
)
session.mount("https://", HTTPAdapter(max_retries=retries))


def get_company_tickers() -> pd.DataFrame:
    """Tüm şirketlerin CIK + ticker listesi (~10k şirket)"""
    response = session.get(tickers_url, timeout=30)
    df = pd.DataFrame.from_dict(response.json(), orient="index")
    df["cik_str"] = df["cik_str"].astype(str).str.zfill(10)
    return df


def get_submissions(cik: str) -> pd.DataFrame:
    """Bir şirketin tüm filing geçmişi"""
    response = session.get(f"{submissions_url}/CIK{cik}.json", timeout=30)
    return pd.DataFrame.from_dict(response.json()["filings"]["recent"])


def get_company_facts(cik: str) -> pd.DataFrame:
    """Bir şirketin tüm XBRL fact'leri (ham)"""
    response = session.get(f"{facts_url}/CIK{cik}.json", timeout=30)
    return pd.DataFrame.from_dict(response.json()["facts"])


def get_company_concept(
    cik: str,
    concept: str = "Revenues",
    taxonomy: str = "us-gaap"
) -> pd.DataFrame:
    """Tek bir konsept için zaman serisi (örn. Revenues, Assets)"""
    response = session.get(
        f"{concepts_url}/CIK{cik}/{taxonomy}/{concept}.json",
        timeout=30
    )
    response.raise_for_status()  # 404, 429 gibi hatalarda exception fırlat
    
    data  = response.json()
    units = data.get("units", {})
    
    if not units:
        return pd.DataFrame()  # boş döndür, pipeline durmasın
    
    if "USD" in units:
        df = pd.DataFrame(units["USD"])
    else:
        first_unit = list(units.keys())[0]
        df = pd.DataFrame(units[first_unit])
    
    df["concept"]  = concept
    df["cik"]      = cik
    df["taxonomy"] = taxonomy
    
    df = df.rename(columns={
        "fp":    "fiscal_period",
        "fy":    "fiscal_year",
        "val":   "value",
        "filed": "filed_date",
        "accn":  "accession_no",
        "end":   "period_end",
        "start": "period_start",
    })
    return df


def get_multiple_concepts(
    cik: str,
    concepts: list[str],
    taxonomy: str = "us-gaap",
    sleep: float = 0.15   # SEC rate limit: max ~10 req/sec
) -> pd.DataFrame:
    """Birden fazla konsepti tek seferde çek, birleştir"""
    frames = []
    for concept in concepts:
        try:
            df = get_company_concept(cik, concept, taxonomy)
            frames.append(df)
            time.sleep(sleep)
        except Exception as e:
            print(f"  [{cik}] {concept} alınamadı: {e}")
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def get_universe_concepts(
    tickers_df: pd.DataFrame,
    concepts: list[str],
    n: int | None = None   # None = tüm universe, 10 = test için ilk 10
) -> pd.DataFrame:
    """
    Tüm universe için konseptleri çek.
    n parametresiyle test için kaç şirket çekileceği sınırlanabilir.
    """
    ciks = tickers_df["cik_str"].tolist()
    if n:
        ciks = ciks[:n]

    all_frames = []
    for i, cik in enumerate(ciks):
        print(f"[{i+1}/{len(ciks)}] CIK: {cik}")
        df = get_multiple_concepts(cik, concepts)
        if not df.empty:
            all_frames.append(df)

    return pd.concat(all_frames, ignore_index=True) if all_frames else pd.DataFrame()