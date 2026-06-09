{{ config(materialized='table') }}

WITH facts AS (
    SELECT
        cik,
        concept,
        value,
        fiscal_year,
        fiscal_period,
        period_end,
        form
    FROM {{ source('raw2', 'raw_company_facts') }}
    WHERE
        concept IN (
            'Revenues',
            'RevenueFromContractWithCustomerExcludingAssessedTax',
            'GrossProfit',
            'OperatingIncomeLoss',
            'NetIncomeLoss',
            'Assets',
            'StockholdersEquity'
        )
        AND form IN ('10-Q', '10-K', '10-Q/A', '10-K/A')
        AND fiscal_year >= EXTRACT(YEAR FROM CURRENT_DATE()) - 10
        AND value IS NOT NULL
),

pivoted AS (
    SELECT
        cik,
        fiscal_year,
        fiscal_period,
        MAX(period_end) AS period_end,
        COALESCE(
            MAX(CASE WHEN concept = 'Revenues' THEN value END),
            MAX(CASE WHEN concept = 'RevenueFromContractWithCustomerExcludingAssessedTax' THEN value END)
        ) AS revenue,
        MAX(CASE WHEN concept = 'GrossProfit'         THEN value END) AS gross_profit,
        MAX(CASE WHEN concept = 'OperatingIncomeLoss' THEN value END) AS operating_income,
        MAX(CASE WHEN concept = 'NetIncomeLoss'       THEN value END) AS net_income,
        MAX(CASE WHEN concept = 'Assets'              THEN value END) AS total_assets,
        MAX(CASE WHEN concept = 'StockholdersEquity'  THEN value END) AS stockholders_equity
    FROM facts
    GROUP BY 1, 2, 3
),

companies AS (
    SELECT cik, ticker, entity_name AS company_name
    FROM {{ source('raw2', 'raw_companies') }}
),

sectors AS (
    SELECT
        ticker,
        gics_sector,
        gics_sub_industry
    FROM {{ source('raw2', 'raw_companies_sector') }}
)

SELECT
    p.*,
    c.ticker,
    c.company_name,
    COALESCE(s.gics_sector, 'Unknown')       AS gics_sector,
    COALESCE(s.gics_sub_industry, 'Unknown') AS gics_sub_industry,
    CASE WHEN p.fiscal_period = 'FY' THEN TRUE ELSE FALSE END AS is_annual
FROM pivoted p
LEFT JOIN companies c USING (cik)
LEFT JOIN sectors s ON c.ticker = s.ticker
WHERE p.revenue IS NOT NULL