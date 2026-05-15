import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.infrastructure.edgar_client import (
    get_company_tickers,
    get_submissions,
    get_company_concept
)
from src.infrastructure.database_manager import get_client
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")

def write_to_bigquery(df, table_name: str, dataset: str = "raw"):
    client = get_client()
    table_id = f"{PROJECT_ID}.{dataset}.{table_name}"
    job = client.load_table_from_dataframe(df, table_id)
    job.result()
    print(f"Yazıldı: {table_id} — {len(df)} satır")

def run_ingestion():
    # 1. Şirket listesini çek ve BigQuery'e yaz
    print("Şirket listesi çekiliyor...")
    companies = get_company_tickers()
    write_to_bigquery(companies, "company_tickers")

    # 2. İlk şirketin CIK'ini al (NVIDIA)
    cik = companies.iloc[0]['cik_str']
    print(f"CIK: {cik}")

    # 3. Submission verilerini çek ve yaz
    print("Submission verileri çekiliyor...")
    submissions = get_submissions(cik)
    write_to_bigquery(submissions, "submissions")

    # 4. Assets verisini çek ve yaz
    print("Assets verisi çekiliyor...")
    assets = get_company_concept(cik, concept="Assets")
    assets_10q = assets[assets['form'] == '10-Q'].reset_index(drop=True)
    write_to_bigquery(assets_10q, "assets_10q")

    print("Tüm veriler BigQuery'e yazıldı!")

if __name__ == "__main__":
    run_ingestion()