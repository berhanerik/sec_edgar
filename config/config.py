import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()

@dataclass
class BigQueryConfig:
    project_id:   str = os.getenv("GCP_PROJECT_ID", "your-gcp-project")
    dataset_raw:  str = os.getenv("BQ_DATASET_RAW",  "raw2")
    dataset_mart: str = os.getenv("BQ_DATASET_MART", "mart2")
    location:     str = os.getenv("BQ_LOCATION",     "EU")

@dataclass
class MLConfig:
    revenue_target_col: str = "next_quarter_revenue_growth_pct"
    revenue_mart_table: str = "mart_revenue_panel"
    health_feature_cols: list = field(default_factory=lambda: [
        "profitability_score",
        "leverage_ratio",
        "liquidity_ratio",
        "roe",
        "capex_intensity",
    ])
    health_mart_table: str = "mart_company_health"
    n_clusters_kmeans: int = 5
    dbscan_eps: float = 0.5
    dbscan_min_samples: int = 5
    test_size: float = 0.2
    random_state: int = 42
    cv_folds: int = 5

@dataclass
class Config:
    bq: BigQueryConfig = field(default_factory=BigQueryConfig)
    ml: MLConfig = field(default_factory=MLConfig)
    log_level: str = "INFO"
    universe: str = "sp500_russell1000"
    lookback_years: int = 10