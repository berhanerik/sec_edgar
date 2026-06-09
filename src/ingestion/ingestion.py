from __future__ import annotations
import logging
import time
from datetime import datetime
import pandas as pd
from google.cloud import bigquery
from config.config import Config
from src.infrastructure.edgar_client import get_multiple_concepts
from src.ingestion.universe import get_universe
from src.ingestion.schemas import (
    RAW_COMPANY_FACTS_TABLE_CONFIG,
    RAW_COMPANIES_SCHEMA,
)

logger = logging.getLogger(__name__)

CONCEPTS = [
    "Revenues",
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "GrossProfit",
    "OperatingIncomeLoss",
    "NetIncomeLoss",
    "Assets",
    "Liabilities",
    "StockholdersEquity",
    "AssetsCurrent",
    "LiabilitiesCurrent",
    "PaymentsToAcquirePropertyPlantAndEquipment",
    "NetCashProvidedByUsedInOperatingActivities",
    "EarningsPerShareBasic",
    "EarningsPerShareDiluted",
    "ResearchAndDevelopmentExpense",
]

FORMS_TO_KEEP  = {"10-K", "10-Q", "10-K/A", "10-Q/A"}
LOOKBACK_YEARS = 10


def _now() -> pd.Timestamp:
    return pd.Timestamp.utcnow()


def _align_to_schema(df: pd.DataFrame, schema: list) -> pd.DataFrame:
    """DataFrame kolonlarını BQ şemasıyla eşleştir, tipleri düzelt."""
    expected = [f.name for f in schema]
    type_map = {f.name: f.field_type for f in schema}

    for col in expected:
        if col not in df.columns:
            df[col] = None

    # Tip dönüşümleri
    for col in expected:
        if col not in df.columns:
            continue
        field_type = type_map.get(col, "")
        try:
            if field_type == "DATE":
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
            elif field_type == "TIMESTAMP":
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
            elif field_type == "INT64":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif field_type == "FLOAT64":
                df[col] = pd.to_numeric(df[col], errors="coerce")
            elif field_type == "BOOL":
                df[col] = df[col].astype("boolean")
            elif field_type == "STRING":
                df[col] = df[col].astype(str).where(df[col].notna(), None)
        except Exception:
            pass

    return df[expected]


def _filter_facts(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    cutoff = datetime.now().year - LOOKBACK_YEARS
    if "form" in df.columns:
        df = df[df["form"].isin(FORMS_TO_KEEP)]
    if "fiscal_year" in df.columns:
        df = df[df["fiscal_year"].fillna(0) >= cutoff]
    return df


def _write_to_bq(
    bq: bigquery.Client,
    df: pd.DataFrame,
    table_full: str,
    schema: list,
) -> None:
    if df.empty:
        return
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_APPEND",
    )
    job = bq.load_table_from_dataframe(df, table_full, job_config=job_config)
    job.result()
    logger.info(f"  {len(df):,} satır yazıldı → {table_full}")


def run_ingestion(config: Config) -> None:
    logger.info("── Stage 1: EDGAR Ingestion ─────────────────────────────")

    bq        = bigquery.Client(project=config.bq.project_id)
    project   = config.bq.project_id
    ds        = config.bq.dataset_raw
    facts_tbl = f"{project}.{ds}.raw_company_facts"
    comp_tbl  = f"{project}.{ds}.raw_companies"

    # ── Universe ──────────────────────────────────────────────────────
    universe_df = get_universe()
    logger.info(f"Universe: {len(universe_df)} şirket")

    # ── raw_companies yaz ─────────────────────────────────────────────
    companies_df = universe_df.copy()
    companies_df = companies_df.rename(columns={
        "cik_str": "cik",
        "title":   "entity_name",
    })
    companies_df["ingested_at"] = _now()
    companies_df = _align_to_schema(companies_df, RAW_COMPANIES_SCHEMA)
    _write_to_bq(bq, companies_df, comp_tbl, RAW_COMPANIES_SCHEMA)

    # ── Her şirket için konseptleri çek, raw_company_facts'e yaz ──────
    ciks       = universe_df["cik_str"].tolist()
    BATCH_SIZE = 50

    batch_frames = []

    for i, cik in enumerate(ciks):
        ticker = universe_df[universe_df["cik_str"] == cik]["ticker"].values[0]
        logger.info(f"[{i+1}/{len(ciks)}] {ticker} ({cik})")

        try:
            df = get_multiple_concepts(cik, CONCEPTS, sleep=0.15)
            df = _filter_facts(df)
            if not df.empty:
                df["ingested_at"] = _now()
                batch_frames.append(df)
        except Exception as e:
            logger.warning(f"  {ticker} atlandı: {e}")

        if len(batch_frames) >= BATCH_SIZE:
            combined = pd.concat(batch_frames, ignore_index=True)
            combined = _align_to_schema(
                combined, RAW_COMPANY_FACTS_TABLE_CONFIG["schema"])
            _write_to_bq(bq, combined, facts_tbl,
                         RAW_COMPANY_FACTS_TABLE_CONFIG["schema"])
            batch_frames = []

        time.sleep(0.1)

    # Kalan batch'i yaz
    if batch_frames:
        combined = pd.concat(batch_frames, ignore_index=True)
        combined = _align_to_schema(
            combined, RAW_COMPANY_FACTS_TABLE_CONFIG["schema"])
        _write_to_bq(bq, combined, facts_tbl,
                     RAW_COMPANY_FACTS_TABLE_CONFIG["schema"])

    logger.info("── Stage 1 tamamlandı ───────────────────────────────────")