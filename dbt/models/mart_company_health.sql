{{ config(materialized='table') }}

WITH facts AS (
    SELECT
        cik,
        concept,
        value,
        fiscal_year,
        period_end,
        form
    FROM {{ source('raw2', 'raw_company_facts') }}
    WHERE
        concept IN (
            'Revenues',
            'RevenueFromContractWithCustomerExcludingAssessedTax',
            'GrossProfit',
            'NetIncomeLoss',
            'Assets',
            'Liabilities',
            'StockholdersEquity',
            'AssetsCurrent',
            'LiabilitiesCurrent',
            'PaymentsToAcquirePropertyPlantAndEquipment',
            'NetCashProvidedByUsedInOperatingActivities'
        )
        AND form IN ('10-K', '10-K/A')
        AND fiscal_year >= EXTRACT(YEAR FROM CURRENT_DATE()) - 10
        AND value IS NOT NULL
),

pivoted AS (
    SELECT
        cik,
        fiscal_year,
        COALESCE(
            MAX(CASE WHEN concept = 'Revenues' THEN value END),
            MAX(CASE WHEN concept = 'RevenueFromContractWithCustomerExcludingAssessedTax' THEN value END)
        ) AS revenue,
        MAX(CASE WHEN concept = 'GrossProfit'                                         THEN value END) AS gross_profit,
        MAX(CASE WHEN concept = 'NetIncomeLoss'                                       THEN value END) AS net_income,
        MAX(CASE WHEN concept = 'Assets'                                              THEN value END) AS total_assets,
        MAX(CASE WHEN concept = 'Liabilities'                                         THEN value END) AS total_liabilities,
        MAX(CASE WHEN concept = 'StockholdersEquity'                                  THEN value END) AS stockholders_equity,
        MAX(CASE WHEN concept = 'AssetsCurrent'                                       THEN value END) AS current_assets,
        MAX(CASE WHEN concept = 'LiabilitiesCurrent'                                  THEN value END) AS current_liabilities,
        MAX(CASE WHEN concept = 'PaymentsToAcquirePropertyPlantAndEquipment'          THEN value END) AS capital_expenditures,
        MAX(CASE WHEN concept = 'NetCashProvidedByUsedInOperatingActivities'          THEN value END) AS operating_cash_flow
    FROM facts
    GROUP BY 1, 2
),

companies AS (
    SELECT cik, ticker, entity_name AS company_name
    FROM {{ source('raw2', 'raw_companies') }}
)

SELECT
    p.*,
    c.ticker,
    c.company_name
FROM pivoted p
LEFT JOIN companies c USING (cik)
WHERE p.revenue IS NOT NULL
  AND p.total_assets IS NOT NULL