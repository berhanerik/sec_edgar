SELECT
    report_date,
    asset_value,
    fiscal_year,
    fiscal_period,
    LAG(asset_value) OVER (ORDER BY report_date) AS prev_quarter_value,
    ROUND(
        (asset_value - LAG(asset_value) OVER (ORDER BY report_date)) 
        / LAG(asset_value) OVER (ORDER BY report_date) * 100, 2
    ) AS qoq_growth_pct
FROM {{ ref('stg_assets') }}