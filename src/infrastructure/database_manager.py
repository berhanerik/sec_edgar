import logging
import pandas as pd
from google.cloud import bigquery
from config.config import Config

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, config: Config):
        self.config = config
        self.client = bigquery.Client(
            project=config.bq.project_id,
            location=config.bq.location,   # EU
        )

    def read_mart(self, table_name: str) -> pd.DataFrame:
        full = f"{self.config.bq.project_id}.{self.config.bq.dataset_mart}.{table_name}"
        logger.info(f"Reading {full}")
        return self.client.query(f"SELECT * FROM `{full}`").to_dataframe()

    def run_query(self, sql: str) -> pd.DataFrame:
        return self.client.query(sql).to_dataframe()

    def write_ml_results(self, df: pd.DataFrame, table_name: str,
                         write_disposition: str = "WRITE_TRUNCATE") -> None:
        full = f"{self.config.bq.project_id}.ml.{table_name}"
        logger.info(f"Writing {len(df):,} rows → {full}")
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            autodetect=True,
        )
        job = self.client.load_table_from_dataframe(df, full, job_config=job_config)
        job.result()
        logger.info(f"Write complete: {full}")