SELECT
    `end` AS report_date,
    val AS asset_value,
    form,
    filed,
    fy AS fiscal_year,
    fp AS fiscal_period
FROM {{ source('raw', 'assets_10q') }}
WHERE val IS NOT NULL
ORDER BY report_date ASC