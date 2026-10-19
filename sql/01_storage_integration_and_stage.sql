-- ==============================================================================
-- 01: STORAGE INTEGRATION AND STAGE SETUP
-- ==============================================================================
-- Secures Snowflake cloud connectivity using cloud IAM roles without long-lived keys.

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE STORAGE INTEGRATION s3_hdb_resale_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake-hdb-ingestion-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://my-enterprise-data-lake-prod/raw/hdb_resale/');

GRANT USAGE ON INTEGRATION s3_hdb_resale_integration TO ROLE DATA_ENGINEER_ROLE;

USE ROLE DATA_ENGINEER_ROLE;
CREATE DATABASE IF NOT EXISTS ENTERPRISE_DW;
CREATE SCHEMA IF NOT EXISTS ENTERPRISE_DW.RAW;
CREATE SCHEMA IF NOT EXISTS ENTERPRISE_DW.CONFORMED;
CREATE SCHEMA IF NOT EXISTS ENTERPRISE_DW.MARTS;

USE SCHEMA ENTERPRISE_DW.RAW;

CREATE OR REPLACE FILE FORMAT parquet_snappy_format
  TYPE = PARQUET
  COMPRESSION = SNAPPY
  BINARY_AS_TEXT = FALSE
  TRIM_SPACE = TRUE;

CREATE OR REPLACE STAGE hdb_resale_parquet_stage
  STORAGE_INTEGRATION = s3_hdb_resale_integration
  URL = 's3://my-enterprise-data-lake-prod/raw/hdb_resale/'
  FILE_FORMAT = parquet_snappy_format;
