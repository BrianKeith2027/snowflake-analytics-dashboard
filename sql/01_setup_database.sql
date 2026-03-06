-- ============================================================
-- Snowflake Analytics Dashboard - Database Setup
-- Author: Brian Stratton
-- Description: Creates the database, warehouse, schemas, and
--              role structure for the retail analytics platform.
-- ============================================================

-- Create a dedicated warehouse for analytics workloads
CREATE WAREHOUSE IF NOT EXISTS ANALYTICS_WH
    WITH WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for retail analytics dashboard queries';

-- Create the analytics database
CREATE DATABASE IF NOT EXISTS RETAIL_ANALYTICS
    COMMENT = 'Retail & supply chain analytics platform';

USE DATABASE RETAIL_ANALYTICS;

-- Create Medallion Architecture schemas
CREATE SCHEMA IF NOT EXISTS BRONZE
    COMMENT = 'Raw data ingestion layer - landing zone for source data';

CREATE SCHEMA IF NOT EXISTS SILVER
    COMMENT = 'Cleaned and validated data layer - business-ready tables';

CREATE SCHEMA IF NOT EXISTS GOLD
    COMMENT = 'Aggregated analytics layer - dashboard-ready views and tables';

CREATE SCHEMA IF NOT EXISTS STAGING
    COMMENT = 'Temporary staging area for file ingestion';

-- Create file format for CSV ingestion
CREATE OR REPLACE FILE FORMAT STAGING.CSV_FORMAT
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '')
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    COMMENT = 'Standard CSV file format for data ingestion';

-- Create internal stage for file uploads
CREATE OR REPLACE STAGE STAGING.DATA_STAGE
    FILE_FORMAT = STAGING.CSV_FORMAT
    COMMENT = 'Internal stage for uploading CSV data files';

-- Grant usage
GRANT USAGE ON WAREHOUSE ANALYTICS_WH TO ROLE SYSADMIN;
GRANT ALL ON DATABASE RETAIL_ANALYTICS TO ROLE SYSADMIN;

-- Verify setup
SHOW SCHEMAS IN DATABASE RETAIL_ANALYTICS;
SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE();
