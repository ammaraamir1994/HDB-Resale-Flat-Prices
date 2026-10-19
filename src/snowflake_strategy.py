"""Production Snowflake Loading Strategy: DDL, Ingestion Architecture, and SQL Templates."""

from typing import Dict

# Production SQL Templates implementing the Medallion Architecture (Bronze -> Silver -> Gold)
# in Snowflake with full idempotency and zero-data-loss schema drift handling.

SNOWFLAKE_STORAGE_INTEGRATION_SQL = """
-- ==============================================================================
-- STEP 1: INFRASTRUCTURE & STORAGE INTEGRATION (Cloud Security Best Practice)
-- ==============================================================================
-- Production architectures avoid hardcoding cloud credentials. Instead, use a Snowflake
-- Storage Integration backed by AWS IAM Role, Azure Service Principal, or GCP Service Account.

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE STORAGE INTEGRATION s3_hdb_resale_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake-hdb-ingestion-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://my-enterprise-data-lake-prod/raw/hdb_resale/');

-- Grant access to the Data Engineering operational role
GRANT USAGE ON INTEGRATION s3_hdb_resale_integration TO ROLE DATA_ENGINEER_ROLE;
"""

SNOWFLAKE_STAGE_AND_FILE_FORMAT_SQL = """
-- ==============================================================================
-- STEP 2: FILE FORMAT & EXTERNAL STAGE SETUP
-- ==============================================================================
USE ROLE DATA_ENGINEER_ROLE;
USE DATABASE ENTERPRISE_DW;
USE SCHEMA RAW;

-- High-performance Parquet format with snappy decompression
CREATE OR REPLACE FILE FORMAT parquet_snappy_format
  TYPE = PARQUET
  COMPRESSION = SNAPPY
  BINARY_AS_TEXT = FALSE
  TRIM_SPACE = TRUE;

-- External stage pointing to the partitioned cloud lake storage
CREATE OR REPLACE STAGE hdb_resale_parquet_stage
  STORAGE_INTEGRATION = s3_hdb_resale_integration
  URL = 's3://my-enterprise-data-lake-prod/raw/hdb_resale/'
  FILE_FORMAT = parquet_snappy_format;
"""

SNOWFLAKE_BRONZE_DDL_SQL = """
-- ==============================================================================
-- STEP 3: BRONZE LAYER (Raw Landing & Schema Drift Absorption)
-- ==============================================================================
-- Strategy: Schema-on-Read using Snowflake's VARIANT data type.
-- By preserving the entire raw JSON/Parquet payload alongside extracted metadata,
-- upstream API schema changes (added/removed columns) NEVER break ingestion.

CREATE OR REPLACE TABLE RAW.BRONZE_HDB_RESALE (
    record_hash VARCHAR(64) NOT NULL,              -- Deterministic SHA-256 surrogate key
    month VARCHAR(7) NOT NULL,                      -- Transaction month (partition identifier)
    extracted_at_utc TIMESTAMP_NTZ NOT NULL,        -- Source extraction timestamp
    source_resource_id VARCHAR(64) NOT NULL,        -- data.gov.sg resource catalog ID
    raw_payload VARIANT NOT NULL,                   -- Complete semi-structured payload (Schema-drift safe)
    staged_file_name VARCHAR NOT NULL,              -- Cloud storage lineage
    staged_file_row_number NUMBER NOT NULL,         -- Source row offset
    ingested_at_utc TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), -- Warehouse ingestion timestamp
    CONSTRAINT pk_bronze_hdb PRIMARY KEY (record_hash)
)
CLUSTER BY (month);
"""

SNOWPIPE_AUTO_INGEST_SQL = """
-- ==============================================================================
-- STEP 4: AUTOMATED CONTINUOUS INGESTION (Snowpipe)
-- ==============================================================================
-- Snowpipe listens to Cloud Event notifications (AWS S3 SQS / SNS / EventBridge)
-- for newly written Parquet files and loads them micro-batch with sub-minute latency.

CREATE OR REPLACE PIPE RAW.PIPE_HDB_RESALE_INGEST
  AUTO_INGEST = TRUE
AS
COPY INTO RAW.BRONZE_HDB_RESALE (
    record_hash,
    month,
    extracted_at_utc,
    source_resource_id,
    raw_payload,
    staged_file_name,
    staged_file_row_number,
    ingested_at_utc
)
FROM (
    SELECT
        $1:_record_hash::VARCHAR,
        $1:month::VARCHAR,
        $1:_extracted_at_utc::TIMESTAMP_NTZ,
        $1:_resource_id::VARCHAR,
        PARSE_JSON($1:_raw_payload),
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER,
        CURRENT_TIMESTAMP()
    FROM @RAW.hdb_resale_parquet_stage
)
FILE_FORMAT = (FORMAT_NAME = 'RAW.parquet_snappy_format')
ON_ERROR = 'SKIP_FILE';
"""

SNOWFLAKE_SILVER_MERGE_SQL = """
-- ==============================================================================
-- STEP 5: SILVER CONFORMED LAYER (Idempotent MERGE Transformation)
-- ==============================================================================
-- Implements idempotent upsert from Bronze to Silver.
-- Re-running for an existing month updates existing records and inserts new ones,
-- guaranteeing ZERO DUPLICATE ROWS even if upstream files are re-ingested.

CREATE TABLE IF NOT EXISTS CONFORMED.SILVER_HDB_RESALE_PRICES (
    record_hash VARCHAR(64) NOT NULL,
    transaction_month VARCHAR(7) NOT NULL,
    town VARCHAR(64) NOT NULL,
    flat_type VARCHAR(32) NOT NULL,
    block VARCHAR(16) NOT NULL,
    street_name VARCHAR(128) NOT NULL,
    storey_range VARCHAR(32) NOT NULL,
    floor_area_sqm NUMBER(10, 2) NOT NULL,
    flat_model VARCHAR(64) NOT NULL,
    lease_commence_year NUMBER(4, 0) NOT NULL,
    remaining_lease VARCHAR(64),                  -- Nullable to accommodate pre-2015 data
    resale_price_sgd NUMBER(12, 2) NOT NULL,
    price_per_sqm NUMBER(12, 2) AS (resale_price_sgd / NULLIF(floor_area_sqm, 0)),
    source_resource_id VARCHAR(64) NOT NULL,
    extracted_at_utc TIMESTAMP_NTZ NOT NULL,
    updated_at_utc TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_silver_hdb PRIMARY KEY (record_hash)
)
CLUSTER BY (transaction_month, town);

-- Idempotent Batch Upsert (Can be scheduled via Snowflake Streams & Tasks or dbt)
MERGE INTO CONFORMED.SILVER_HDB_RESALE_PRICES AS tgt
USING (
    SELECT
        record_hash,
        month AS transaction_month,
        UPPER(TRIM(raw_payload:town::VARCHAR)) AS town,
        UPPER(TRIM(raw_payload:flat_type::VARCHAR)) AS flat_type,
        TRIM(raw_payload:block::VARCHAR) AS block,
        UPPER(TRIM(raw_payload:street_name::VARCHAR)) AS street_name,
        UPPER(TRIM(raw_payload:storey_range::VARCHAR)) AS storey_range,
        TRY_TO_NUMBER(raw_payload:floor_area_sqm::VARCHAR, 10, 2) AS floor_area_sqm,
        TRIM(raw_payload:flat_model::VARCHAR) AS flat_model,
        TRY_TO_NUMBER(raw_payload:lease_commence_date::VARCHAR, 4, 0) AS lease_commence_year,
        TRIM(raw_payload:remaining_lease::VARCHAR) AS remaining_lease,
        TRY_TO_NUMBER(raw_payload:resale_price::VARCHAR, 12, 2) AS resale_price_sgd,
        source_resource_id,
        extracted_at_utc
    FROM RAW.BRONZE_HDB_RESALE
    -- Filter on partition month for targeted reprocessing or run full incremental
    QUALIFY ROW_NUMBER() OVER (PARTITION BY record_hash ORDER BY extracted_at_utc DESC) = 1
) AS src
ON tgt.record_hash = src.record_hash
WHEN MATCHED THEN UPDATE SET
    tgt.resale_price_sgd = src.resale_price_sgd,
    tgt.remaining_lease = src.remaining_lease,
    tgt.updated_at_utc = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    record_hash, transaction_month, town, flat_type, block,
    street_name, storey_range, floor_area_sqm, flat_model,
    lease_commence_year, remaining_lease, resale_price_sgd,
    source_resource_id, extracted_at_utc, updated_at_utc
) VALUES (
    src.record_hash, src.transaction_month, src.town, src.flat_type, src.block,
    src.street_name, src.storey_range, src.floor_area_sqm, src.flat_model,
    src.lease_commence_year, src.remaining_lease, src.resale_price_sgd,
    src.source_resource_id, src.extracted_at_utc, CURRENT_TIMESTAMP()
);
"""

SNOWFLAKE_GOLD_ANALYTICS_SQL = """
-- ==============================================================================
-- STEP 6: GOLD ANALYTICAL MARTS (Aggregations for BI / Reporting)
-- ==============================================================================
-- Implemented as Snowflake Dynamic Tables with target lag for automated incremental refresh.

CREATE OR REPLACE DYNAMIC TABLE MARTS.GOLD_HDB_MONTHLY_MARKET_TRENDS
  TARGET_LAG = '1 HOUR'
  WAREHOUSE = ANALYTICS_WH
AS
SELECT
    transaction_month,
    town,
    flat_type,
    COUNT(*) AS total_transactions,
    ROUND(AVG(resale_price_sgd), 2) AS avg_resale_price_sgd,
    ROUND(MEDIAN(resale_price_sgd), 2) AS median_resale_price_sgd,
    ROUND(MIN(resale_price_sgd), 2) AS min_resale_price_sgd,
    ROUND(MAX(resale_price_sgd), 2) AS max_resale_price_sgd,
    ROUND(AVG(price_per_sqm), 2) AS avg_price_per_sqm,
    ROUND(AVG(floor_area_sqm), 1) AS avg_floor_area_sqm
FROM CONFORMED.SILVER_HDB_RESALE_PRICES
GROUP BY transaction_month, town, flat_type;
"""


def get_all_sql_scripts() -> Dict[str, str]:
    """Returns all Snowflake SQL setup and loading scripts."""
    return {
        "01_storage_integration.sql": SNOWFLAKE_STORAGE_INTEGRATION_SQL,
        "02_stage_and_file_format.sql": SNOWFLAKE_STAGE_AND_FILE_FORMAT_SQL,
        "03_bronze_raw_table.sql": SNOWFLAKE_BRONZE_DDL_SQL,
        "04_snowpipe_ingestion.sql": SNOWPIPE_AUTO_INGEST_SQL,
        "05_silver_idempotent_merge.sql": SNOWFLAKE_SILVER_MERGE_SQL,
        "06_gold_dynamic_table.sql": SNOWFLAKE_GOLD_ANALYTICS_SQL,
    }
