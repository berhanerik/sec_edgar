from __future__ import annotations
import logging
from datetime import datetime
from typing import Tuple
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import ElasticNet, LinearRegression, Ridge
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import KFold, cross_val_score, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from config.config import Config
from src.infrastructure.database_manager import DatabaseManager

try:
    from xgboost import XGBRegressor
    XGBOOST_AVAILABLE = True
except ImportError:
    XGBOOST_AVAILABLE = False

logger = logging.getLogger(__name__)


def build_features(df: pd.DataFrame, config: Config) -> pd.DataFrame:
    df = df.copy()
    if "fiscal_period" in df.columns:
        df = df.rename(columns={"fiscal_period": "fiscal_quarter"})
    if "gics_sector" not in df.columns:
        df["gics_sector"] = "Unknown"

    df = df.sort_values(["cik", "fiscal_year", "fiscal_quarter"])
    df["revenue_growth"]      = df.groupby("cik")["revenue"].pct_change() * 100
    df[config.ml.revenue_target_col] = df.groupby("cik")["revenue_growth"].shift(-1)
    df["revenue_growth_lag1"] = df.groupby("cik")["revenue_growth"].shift(1)
    df["revenue_growth_lag2"] = df.groupby("cik")["revenue_growth"].shift(2)
    df["revenue_growth_lag3"] = df.groupby("cik")["revenue_growth"].shift(3)
    df["gross_margin"]        = df["gross_profit"]     / df["revenue"].replace(0, np.nan)
    df["operating_margin"]    = df["operating_income"] / df["revenue"].replace(0, np.nan)
    df["asset_turnover"]      = df["revenue"]          / df["total_assets"].replace(0, np.nan)
    df["net_margin"]          = df["net_income"]       / df["revenue"].replace(0, np.nan)
    df["equity_ratio"]        = df["stockholders_equity"] / df["total_assets"].replace(0, np.nan)

    for col in ["revenue_growth", "revenue_growth_lag1", "revenue_growth_lag2",
                "revenue_growth_lag3", config.ml.revenue_target_col]:
        df[col] = df[col].clip(-200, 200)
    return df


def build_pipeline(numeric_features, cat_features, model) -> Pipeline:
    transformers = [("num", StandardScaler(), numeric_features)]
    if cat_features:
        transformers.append(
            ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), cat_features)
        )
    preprocessor = ColumnTransformer(transformers)
    return Pipeline([("prep", preprocessor), ("model", model)])


def get_models() -> dict:
    models = {
        "linear_regression":   LinearRegression(),
        "ridge":               Ridge(alpha=1.0),
        "elastic_net":         ElasticNet(alpha=0.1, l1_ratio=0.5, max_iter=10_000),
        "random_forest":       RandomForestRegressor(
                                    n_estimators=100,
                                    max_depth=6,
                                    random_state=42,
                                    n_jobs=-1,
                               ),
        "gradient_boosting":   GradientBoostingRegressor(
                                    n_estimators=100,
                                    max_depth=4,
                                    learning_rate=0.1,
                                    random_state=42,
                               ),
    }
    if XGBOOST_AVAILABLE:
        models["xgboost"] = XGBRegressor(
            n_estimators=100,
            max_depth=4,
            learning_rate=0.1,
            random_state=42,
            n_jobs=-1,
            verbosity=0,
        )
    return models


def train_and_evaluate(
    df: pd.DataFrame, config: Config
) -> Tuple[Pipeline, pd.DataFrame, pd.DataFrame]:
    target       = config.ml.revenue_target_col
    num_features = [
        "revenue_growth_lag1", "revenue_growth_lag2", "revenue_growth_lag3",
        "gross_margin", "operating_margin", "asset_turnover",
        "net_margin", "equity_ratio",
    ]
    cat_features = ["fiscal_quarter", "gics_sector"]
    all_features = num_features + cat_features

    ml_df = df.dropna(subset=[target] + num_features + ["fiscal_quarter"]).copy()
    ml_df["gics_sector"] = ml_df["gics_sector"].fillna("Unknown")
    logger.info(f"Eğitim satırı: {len(ml_df):,}")

    X = ml_df[all_features]
    y = ml_df[target]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=config.ml.test_size,
        random_state=config.ml.random_state,
    )
    kf = KFold(
        n_splits=config.ml.cv_folds,
        shuffle=True,
        random_state=config.ml.random_state,
    )

    metrics_rows     = []
    fitted_pipelines = {}

    for name, estimator in get_models().items():
        logger.info(f"{name} eğitiliyor...")
        pipe    = build_pipeline(num_features, cat_features, estimator)
        cv_r2   = cross_val_score(pipe, X_train, y_train, cv=kf, scoring="r2")
        cv_rmse = np.sqrt(-cross_val_score(
            pipe, X_train, y_train, cv=kf,
            scoring="neg_mean_squared_error",
        ))
        pipe.fit(X_train, y_train)
        y_pred = pipe.predict(X_test)
        r2   = r2_score(y_test, y_pred)
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        mae  = mean_absolute_error(y_test, y_pred)

        metrics_rows.append({
            "model_name":    name,
            "cv_r2_mean":    round(cv_r2.mean(), 4),
            "cv_r2_std":     round(cv_r2.std(), 4),
            "cv_rmse_mean":  round(cv_rmse.mean(), 4),
            "test_r2":       round(r2, 4),
            "test_rmse":     round(rmse, 4),
            "test_mae":      round(mae, 4),
            "train_rows":    len(X_train),
            "test_rows":     len(X_test),
            "run_timestamp": datetime.utcnow().isoformat(),
        })
        fitted_pipelines[name] = pipe
        logger.info(f"{name}: R²={r2:.4f}  RMSE={rmse:.4f}  MAE={mae:.4f}")

    metrics_df = pd.DataFrame(metrics_rows)
    best_name  = metrics_df.loc[metrics_df["test_r2"].idxmax(), "model_name"]
    logger.info(f"En iyi model: {best_name}")
    best_pipe  = fitted_pipelines[best_name]

    full_preds     = best_pipe.predict(ml_df[all_features])
    predictions_df = ml_df[["cik", "ticker", "fiscal_year", "fiscal_quarter", "gics_sector"]].copy()
    predictions_df["actual_next_q_growth"]    = y.loc[ml_df.index].values
    predictions_df["predicted_next_q_growth"] = full_preds
    predictions_df["residual"]                = (
        predictions_df["actual_next_q_growth"]
        - predictions_df["predicted_next_q_growth"]
    )
    predictions_df["best_model"]    = best_name
    predictions_df["run_timestamp"] = datetime.utcnow().isoformat()

    return best_pipe, predictions_df, metrics_df


def run_revenue_model(config: Config, db: DatabaseManager) -> None:
    logger.info("── Stage 4a: Revenue Growth Model ──────────────────────")
    df = db.read_mart(config.ml.revenue_mart_table)
    df = build_features(df, config)
    _, predictions_df, metrics_df = train_and_evaluate(df, config)
    db.write_ml_results(predictions_df, "ml_revenue_predictions")
    db.write_ml_results(metrics_df,     "ml_revenue_model_metrics")
    logger.info("── Revenue model tamamlandı ─────────────────────────────")