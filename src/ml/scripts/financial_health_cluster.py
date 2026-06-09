from __future__ import annotations
import logging
from datetime import datetime
from typing import Tuple
import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN, KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import RobustScaler
from config.config import Config
from src.infrastructure.database_manager import DatabaseManager

logger = logging.getLogger(__name__)


def build_health_features(df: pd.DataFrame, config: Config) -> pd.DataFrame:
    df  = df.copy().sort_values(["cik", "fiscal_year"])
    eps = 1e-6

    df["gross_margin"]        = df["gross_profit"]  / (df["revenue"] + eps)
    df["net_margin"]          = df["net_income"]     / (df["revenue"] + eps)
    df["profitability_score"] = (df["gross_margin"]  + df["net_margin"]) / 2
    df["leverage_ratio"]      = df["total_liabilities"] / (df["stockholders_equity"].abs() + eps)
    df["liquidity_ratio"]     = df["current_assets"] / (df["current_liabilities"] + eps)
    df["roe"]                 = df["net_income"] / (df["stockholders_equity"].abs() + eps)
    df["capex_intensity"]     = df["capital_expenditures"].abs() / (df["revenue"] + eps)

    clip_rules = {
        "leverage_ratio":      (0, 8),
        "liquidity_ratio":     (0, 4),
        "roe":                 (-0.5, 1.5),
        "capex_intensity":     (0, 0.3),
        "profitability_score": (-0.3, 0.7),
    }
    for col, (lo, hi) in clip_rules.items():
        df[col] = df[col].clip(lo, hi)

    return df


def _tune_kmeans(X_scaled: np.ndarray, k_range: range, random_state: int) -> Tuple[int, float]:
    best_k, best_sil = k_range.start, -1.0
    for k in k_range:
        km     = KMeans(n_clusters=k, random_state=random_state, n_init=10)
        labels = km.fit_predict(X_scaled)
        sil    = silhouette_score(X_scaled, labels,
                                  sample_size=min(5000, len(X_scaled)))
        logger.info(f"  k={k}  silhouette={sil:.4f}")
        if sil > best_sil:
            best_sil, best_k = sil, k
    return best_k, best_sil


def _label_clusters(profiles: pd.DataFrame) -> dict:
    labels = {}
    for _, row in profiles.iterrows():
        cid  = int(row["cluster_id"])
        prof = row["profitability_score"]
        lev  = row["leverage_ratio"]
        liq  = row["liquidity_ratio"]
        roe  = row["roe"]
        capex = row["capex_intensity"]

        if liq > 2.0 and lev < 1.5 and prof > 0.25:
            label = "STRONG_BALANCE_SHEET"
        elif prof > 0.15 and lev < 2.0 and liq > 1.5:
            label = "FINANCIALLY_HEALTHY"
        elif roe > 0.5 and lev > 3.0:
            label = "HIGH_LEVERAGE_PROFITABLE"
        elif prof > 0.05 and roe > 0.1 and liq > 1.0:
            label = "STABLE_GROWER"
        elif lev > 3 or liq < 0.8:
            label = "HIGH_RISK_LEVERAGED"
        elif prof < -0.05:
            label = "DISTRESSED"
        else:
            label = f"MIXED_PROFILE_{cid}"
        labels[cid] = label
    return labels


def run_clustering(
    df: pd.DataFrame, config: Config
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    feature_cols = config.ml.health_feature_cols

    latest = (
        df.sort_values("fiscal_year")
        .groupby("cik")
        .last()
        .reset_index()
    )
    ml_df = latest.dropna(subset=feature_cols).copy()
    logger.info(f"Clustering şirket sayısı: {len(ml_df):,}")

    X        = ml_df[feature_cols].values
    scaler   = RobustScaler()
    X_scaled = scaler.fit_transform(X)

    logger.info("K-Means tuning k=3..8 ...")
    best_k, best_sil = _tune_kmeans(
        X_scaled, range(3, 9), config.ml.random_state
    )
    km        = KMeans(n_clusters=best_k, random_state=config.ml.random_state, n_init=20)
    km_labels = km.fit_predict(X_scaled)

    db        = DBSCAN(eps=2.0, min_samples=3, n_jobs=-1)
    db_labels = db.fit_predict(X_scaled)
    n_noise   = int((db_labels == -1).sum())
    logger.info(f"DBSCAN noise: {n_noise} şirket")

    ml_df["kmeans_cluster"] = km_labels
    ml_df["dbscan_label"]   = db_labels

    ml_df["risk_score"] = (
        ml_df["leverage_ratio"].rank(pct=True)              * 0.35
        + (1 - ml_df["liquidity_ratio"].rank(pct=True))     * 0.25
        + (1 - ml_df["profitability_score"].rank(pct=True)) * 0.25
        + ml_df["roe"].rank(pct=True)                       * 0.15
    ) * 100

    agg        = {col: "mean" for col in feature_cols}
    agg["cik"] = "count"
    profiles   = (
        ml_df.groupby("kmeans_cluster").agg(agg)
        .rename(columns={"cik": "company_count"})
        .reset_index()
        .rename(columns={"kmeans_cluster": "cluster_id"})
    )
    profiles[feature_cols]    = profiles[feature_cols].round(4)
    label_map                 = _label_clusters(profiles)
    profiles["cluster_label"] = profiles["cluster_id"].map(label_map)
    ml_df["cluster_label"]    = ml_df["kmeans_cluster"].map(label_map)

    ts = datetime.utcnow().isoformat()

    clusters_df = ml_df[[
        "cik", "ticker", "fiscal_year",
        "kmeans_cluster", "cluster_label",
        "dbscan_label", "risk_score", *feature_cols,
    ]].copy()
    clusters_df.rename(columns={"kmeans_cluster": "cluster_id"}, inplace=True)
    clusters_df["run_timestamp"] = ts

    profiles_df                  = profiles.copy()
    profiles_df["best_k"]        = best_k
    profiles_df["run_timestamp"] = ts

    metrics_df = pd.DataFrame([{
        "model":            "kmeans",
        "best_k":           best_k,
        "silhouette":       round(best_sil, 4),
        "inertia":          round(km.inertia_, 2),
        "dbscan_noise_n":   n_noise,
        "dbscan_noise_pct": round(n_noise / len(ml_df) * 100, 2),
        "total_companies":  len(ml_df),
        "run_timestamp":    ts,
    }])

    return clusters_df, profiles_df, metrics_df


def run_health_cluster(config: Config, db: DatabaseManager) -> None:
    logger.info("── Stage 4b: Financial Health Clustering ────────────────")
    df = db.read_mart(config.ml.health_mart_table)
    df = build_health_features(df, config)
    clusters_df, profiles_df, metrics_df = run_clustering(df, config)
    db.write_ml_results(clusters_df, "ml_company_clusters")
    db.write_ml_results(profiles_df, "ml_cluster_profiles")
    db.write_ml_results(metrics_df,  "ml_clustering_metrics")
    logger.info("── Clustering tamamlandı ────────────────────────────────")