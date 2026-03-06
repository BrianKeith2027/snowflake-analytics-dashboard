-- ============================================================
-- Snowflake Analytics Dashboard - Table Definitions
-- Author: Brian Stratton
-- Description: Creates Bronze and Silver layer tables for the
--              retail analytics data model.
-- ============================================================

USE DATABASE RETAIL_ANALYTICS;
USE WAREHOUSE ANALYTICS_WH;

-- =====================
-- BRONZE LAYER TABLES
-- =====================

CREATE OR REPLACE TABLE BRONZE.RAW_TRANSACTIONS (
      row_id              NUMBER AUTOINCREMENT,
      order_id            VARCHAR(50),
      order_date          VARCHAR(50),
      customer_id         VARCHAR(50),
      customer_name       VARCHAR(200),
      segment             VARCHAR(50),
      region              VARCHAR(50),
      state               VARCHAR(100),
      city                VARCHAR(100),
      product_id          VARCHAR(50),
      product_name        VARCHAR(300),
      category            VARCHAR(100),
      sub_category        VARCHAR(100),
      quantity            NUMBER,
      unit_price          FLOAT,
      discount_pct        FLOAT,
      sales_amount        FLOAT,
      profit              FLOAT,
      ship_mode           VARCHAR(50),
      ship_date           VARCHAR(50),
      _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
      _source_file        VARCHAR(500),
      CONSTRAINT pk_raw_transactions PRIMARY KEY (row_id)
  )
COMMENT = 'Raw transaction data from source systems';

CREATE OR REPLACE TABLE BRONZE.RAW_PRODUCTS (
      product_id          VARCHAR(50),
      product_name        VARCHAR(300),
      category            VARCHAR(100),
      sub_category        VARCHAR(100),
      brand               VARCHAR(100),
      supplier            VARCHAR(200),
      unit_cost           FLOAT,
      list_price          FLOAT,
      weight_kg           FLOAT,
      _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
      CONSTRAINT pk_raw_products PRIMARY KEY (product_id)
  )
COMMENT = 'Raw product catalog data';

CREATE OR REPLACE TABLE BRONZE.RAW_WAREHOUSES (
      warehouse_id        VARCHAR(20),
      warehouse_name      VARCHAR(100),
      region              VARCHAR(50),
      state               VARCHAR(100),
      city                VARCHAR(100),
      capacity_units      NUMBER,
      manager_name        VARCHAR(200),
      _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
      CONSTRAINT pk_raw_warehouses PRIMARY KEY (warehouse_id)
  )
COMMENT = 'Raw warehouse location and capacity data';

CREATE OR REPLACE TABLE BRONZE.RAW_CUSTOMERS (
      customer_id         VARCHAR(50),
      customer_name       VARCHAR(200),
      segment             VARCHAR(50),
      region              VARCHAR(50),
      state               VARCHAR(100),
      city                VARCHAR(100),
      signup_date         VARCHAR(50),
      loyalty_tier        VARCHAR(50),
      _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
      CONSTRAINT pk_raw_customers PRIMARY KEY (customer_id)
  )
COMMENT = 'Raw customer master data';

-- =====================
-- SILVER LAYER TABLES
-- =====================

CREATE OR REPLACE TABLE SILVER.FACT_SALES (
      sale_id             NUMBER AUTOINCREMENT,
      order_id            VARCHAR(50) NOT NULL,
      sale_date           DATE NOT NULL,
      ship_date           DATE,
      customer_id         VARCHAR(50) NOT NULL,
      product_id          VARCHAR(50) NOT NULL,
      warehouse_id        VARCHAR(20),
      region              VARCHAR(50),
      state_code          VARCHAR(10),
      state_name          VARCHAR(100),
      quantity            NUMBER NOT NULL,
      unit_price          FLOAT NOT NULL,
      discount_pct        FLOAT DEFAULT 0,
      gross_revenue       FLOAT,
      discount_amount     FLOAT,
      net_revenue         FLOAT,
      profit              FLOAT,
      ship_mode           VARCHAR(50),
      days_to_ship        NUMBER,
      sale_year           NUMBER,
      sale_month          NUMBER,
      sale_quarter        NUMBER,
      _processed_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
      CONSTRAINT pk_fact_sales PRIMARY KEY (sale_id)
  )
COMMENT = 'Cleaned and enriched sales fact table';

CREATE OR REPLACE TABLE SILVER.DIM_CUSTOMERS (
      customer_id         VARCHAR(50) NOT NULL,
      customer_name       VARCHAR(200),
      segment             VARCHAR(50),
      region              VARCHAR(50),
      state               VARCHAR(100),
      city                VARCHAR(100),
      signup_date         DATE,
      loyalty_tier        VARCHAR(50),
      customer_segment    VARCHAR(50),
      _processed_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
      CONSTRAINT pk_dim_customers PRIMARY KEY (customer_id)
  )
COMMENT = 'Customer dimension with segmentation';

CREATE OR REPLACE TABLE SILVER.DIM_PRODUCTS (
      product_id          VARCHAR(50) NOT NULL,
      product_name        VARCHAR(300),
      product_category    VARCHAR(100),
      product_subcategory VARCHAR(100),
      brand               VARCHAR(100),
      supplier            VARCHAR(200),
      unit_cost           FLOAT,
      list_price          FLOAT,
      margin_pct          FLOAT,
      _processed_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
      CONSTRAINT pk_dim_products PRIMARY KEY (product_id)
  )
COMMENT = 'Product dimension with margin calculations';

-- Verify tables
SELECT table_schema, table_name, row_count
FROM information_schema.tables
WHERE table_catalog = 'RETAIL_ANALYTICS'
ORDER BY table_schema, table_name;
