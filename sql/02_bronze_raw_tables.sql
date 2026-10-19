-- ==============================================================================
-- 02: BRONZE RAW SCHEMA & SNOWPIPE INGESTION
-- ==============================================================================
-- Uses VARIANT column to achieve schema-on-read resilience against schema drift.
-- Snowpipe triggers automatically on cloud object created events (SQS/EventGrid/PubSub).

USE SCHEMA ENTERPRISE_DW.RAW;

CREATE OR REPLACE TABLE BRONZE_HDB_RESALE (
    record_hash VARCHAR(64) NOT NULL,
    month VARCHAR(7) NOT NULL,
    extracted_at_utc TIMESTAMP_NTZ NOT NULL,
    source_resource_id VARCHAR(64) NOT NULL,
    raw_payload VARIANT NOT NULL,
    staged_file_name VARCHAR NOT NULL,
    staged_file_row_number NUMBER NOT NULL,
    ingested_at_utc TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_bronze_hdb PRIMARY KEY (record_hash)
)
CLUSTER BY (month);

-- Automated Snowpipe definition
CREATE OR REPLACE PIPE PIPE_HDB_RESALE_INGEST
  AUTO_INGEST = TRUE
AS
COPY INTO BRONZE_HDB_RESALE (
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
    FROM @hdb_resale_parquet_stage
)
FILE_FORMAT = (FORMAT_NAME = 'parquet_snappy_format')
ON_ERROR = 'SKIP_FILE';
