-- ==============================================================================
-- 03: SILVER CONFORMED LAYER & IDEMPOTENT MERGE
-- ==============================================================================
-- Idempotent MERGE statement ensuring zero duplication upon repeated pipeline runs.

USE SCHEMA ENTERPRISE_DW.CONFORMED;

CREATE TABLE IF NOT EXISTS SILVER_HDB_RESALE_PRICES (
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
    remaining_lease VARCHAR(64),                  -- Nullable to accommodate schema drift (pre-2015 data)
    resale_price_sgd NUMBER(12, 2) NOT NULL,
    price_per_sqm NUMBER(12, 2) AS (resale_price_sgd / NULLIF(floor_area_sqm, 0)),
    source_resource_id VARCHAR(64) NOT NULL,
    extracted_at_utc TIMESTAMP_NTZ NOT NULL,
    updated_at_utc TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_silver_hdb PRIMARY KEY (record_hash)
)
CLUSTER BY (transaction_month, town);

-- Scheduled / Orchestrated Idempotent Upsert (dbt or Snowflake Task)
MERGE INTO ENTERPRISE_DW.CONFORMED.SILVER_HDB_RESALE_PRICES AS tgt
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
    FROM ENTERPRISE_DW.RAW.BRONZE_HDB_RESALE
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

-- ==============================================================================
-- 04: GOLD ANALYTICS MARTS (Dynamic Table)
-- ==============================================================================
USE SCHEMA ENTERPRISE_DW.MARTS;

CREATE OR REPLACE DYNAMIC TABLE GOLD_HDB_MONTHLY_MARKET_TRENDS
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
FROM ENTERPRISE_DW.CONFORMED.SILVER_HDB_RESALE_PRICES
GROUP BY transaction_month, town, flat_type;
