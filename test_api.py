from config.config import Config
from src.infrastructure.database_manager import DatabaseManager

config = Config()
db     = DatabaseManager(config)

print("=== CLUSTER PROFİLLERİ ===")
df = db.run_query("""
    SELECT cluster_id, cluster_label, company_count,
           ROUND(profitability_score, 3) AS profitability,
           ROUND(leverage_ratio, 3)      AS leverage,
           ROUND(liquidity_ratio, 3)     AS liquidity,
           ROUND(roe, 3)                 AS roe
    FROM `project-0ed3a844-6161-46a7-823.ml.ml_cluster_profiles`
    ORDER BY cluster_id
""")
print(df.to_string())

print("\n=== CLUSTER DAĞILIMI ===")
df2 = db.run_query("""
    SELECT cluster_label, COUNT(*) as sirket_sayisi,
           ROUND(AVG(risk_score), 1) AS avg_risk
    FROM `project-0ed3a844-6161-46a7-823.ml.ml_company_clusters`
    GROUP BY cluster_label
    ORDER BY avg_risk DESC
""")
print(df2.to_string())