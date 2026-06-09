import argparse
import logging
import subprocess
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass

from config.config import Config
from src.infrastructure.logger import setup_logging
from src.infrastructure.database_manager import DatabaseManager
from src.ml.scripts.revenue_growth_model import run_revenue_model
from src.ml.scripts.financial_health_cluster import run_health_cluster

logger = logging.getLogger("run")


def cmd_ingest(config: Config) -> None:
    from src.ingestion.ingestion import run_ingestion
    run_ingestion(config)


def cmd_transform(config: Config) -> None:
    logger.info("dbt run başlatılıyor...")
    result = subprocess.run(
        ["dbt", "run", "--profiles-dir", "dbt", "--project-dir", "dbt"]
    )
    if result.returncode != 0:
        logger.error("dbt run başarısız")
        sys.exit(1)
    subprocess.run(
        ["dbt", "test", "--profiles-dir", "dbt", "--project-dir", "dbt"]
    )
    logger.info("dbt tamamlandı")


def cmd_train(config: Config, db: DatabaseManager) -> None:
    run_revenue_model(config, db)
    run_health_cluster(config, db)


def cmd_pipeline(config: Config, db: DatabaseManager) -> None:
    cmd_ingest(config)
    cmd_transform(config)
    cmd_train(config, db)


def main() -> None:
    parser = argparse.ArgumentParser(description="WIT Data Project Pipeline")
    parser.add_argument(
        "command",
        choices=["ingest", "transform", "train", "pipeline"],
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    setup_logging(level=args.log_level)

    config = Config()
    db     = DatabaseManager(config)

    logger.info(f"Komut: {args.command} | Proje: {config.bq.project_id}")

    dispatch = {
        "ingest":    lambda: cmd_ingest(config),
        "transform": lambda: cmd_transform(config),
        "train":     lambda: cmd_train(config, db),
        "pipeline":  lambda: cmd_pipeline(config, db),
    }
    dispatch[args.command]()
    logger.info("Tamamlandı.")


if __name__ == "__main__":
    main()