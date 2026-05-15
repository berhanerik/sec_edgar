from google.cloud import bigquery
import os
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")

def get_client():
    return bigquery.Client(project=PROJECT_ID)

def create_dataset(dataset_name: str):
    client = get_client()
    dataset_id = f"{PROJECT_ID}.{dataset_name}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "EU"
    client.create_dataset(dataset, exists_ok=True)
    print(f"Dataset oluşturuldu: {dataset_id}")

def test_connection():
    client = get_client()
    print(f"BigQuery bağlantısı başarılı! Proje: {client.project}")

if __name__ == "__main__":
    test_connection()
    create_dataset("raw")
    create_dataset("mart")