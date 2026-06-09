from google.cloud import bigquery

RAW_COMPANY_FACTS_SCHEMA = [
    bigquery.SchemaField('cik',           'STRING',  mode='REQUIRED'),
    bigquery.SchemaField('entity_name',   'STRING'),
    bigquery.SchemaField('namespace',     'STRING'),
    bigquery.SchemaField('concept',       'STRING',  mode='REQUIRED'),
    bigquery.SchemaField('unit',          'STRING'),
    bigquery.SchemaField('period_start',  'DATE'),
    bigquery.SchemaField('period_end',    'DATE',    mode='REQUIRED'),
    bigquery.SchemaField('value',         'FLOAT64', mode='REQUIRED'),
    bigquery.SchemaField('accession_no',  'STRING'),
    bigquery.SchemaField('fiscal_year',   'INT64'),
    bigquery.SchemaField('fiscal_period', 'STRING'),
    bigquery.SchemaField('form',          'STRING'),
    bigquery.SchemaField('filed_date',    'DATE'),
    bigquery.SchemaField('frame',         'STRING'),
    bigquery.SchemaField('ingested_at',   'TIMESTAMP'),
]

RAW_COMPANY_FACTS_TABLE_CONFIG = {
    "schema": RAW_COMPANY_FACTS_SCHEMA,
    "time_partitioning": bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="filed_date",
    ),
    "clustering_fields": ["concept", "cik"],
    "description": "XBRL financial facts from SEC EDGAR companyfacts.zip",
}

RAW_COMPANIES_SCHEMA = [
    bigquery.SchemaField('cik',                    'STRING', mode='REQUIRED'),
    bigquery.SchemaField('entity_name',            'STRING'),
    bigquery.SchemaField('ticker',                 'STRING'),
    bigquery.SchemaField('exchange',               'STRING'),
    bigquery.SchemaField('sic_code',               'STRING'),
    bigquery.SchemaField('sic_description',        'STRING'),
    bigquery.SchemaField('state_of_incorporation', 'STRING'),
    bigquery.SchemaField('fiscal_year_end',        'STRING'),
    bigquery.SchemaField('ein',                    'STRING'),
    bigquery.SchemaField('category',               'STRING'),
    bigquery.SchemaField('former_names',           'STRING'),
    bigquery.SchemaField('ingested_at',            'TIMESTAMP'),
]

RAW_FILINGS_SCHEMA = [
    bigquery.SchemaField('cik',              'STRING', mode='REQUIRED'),
    bigquery.SchemaField('accession_no',     'STRING', mode='REQUIRED'),
    bigquery.SchemaField('form',             'STRING'),
    bigquery.SchemaField('filing_date',      'DATE'),
    bigquery.SchemaField('report_date',      'DATE'),
    bigquery.SchemaField('primary_document', 'STRING'),
    bigquery.SchemaField('is_xbrl',          'BOOL'),
    bigquery.SchemaField('is_inline_xbrl',   'BOOL'),
    bigquery.SchemaField('items',            'STRING'),
    bigquery.SchemaField('size',             'INT64'),
    bigquery.SchemaField('ingested_at',      'TIMESTAMP'),
]
