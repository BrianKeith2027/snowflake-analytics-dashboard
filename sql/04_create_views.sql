-- ============================================================
-- Snowflake Analytics Dashboard - Gold Layer Views
-- Author: Brian Stratton
-- Description: Creates analytics-ready views in the Gold layer
--              powering the Streamlit dashboard visualizations.
-- ============================================================

USE DATABASE RETAIL_ANALYTICS;
USE SCHEMA GOLD;

-- =====================================================
-- 1. DAILY SALES SUMMARY (Revenue Trend Chart)
-- =====================================================
CREATE OR REPLACE VIEW GOLD.DAILY_SALES_SUMMARY AS
WITH daily_agg AS (
      SELECT
          s.sale_date,
          s.region,
          p.product_category,
          COUNT(DISTINCT s.order_id)          AS order_count,
          SUM(s.quantity)                     AS units_sold,
          SUM(s.net_revenue)                  AS total_revenue,
          AVG(s.net_revenue)                  AS avg_order_value,
          SUM(s.discount_amount)              AS total_discounts,
          SUM(s.profit)                       AS total_profit
      FROM SILVER.FACT_SALES s
      JOIN SILVER.DIM_PRODUCTS p ON s.product_id = p.product_id
      GROUP BY s.sale_date, s.region, p.product_category
  ),
with_running AS (
      SELECT
          *,
          SUM(total_revenue) OVER (
              PARTITION BY region
              ORDER BY sale_date
              ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
          ) AS rolling_7d_revenue,
          AVG(total_revenue) OVER (
              PARTITION BY region
              ORDER BY sale_date
              ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
          ) AS rolling_30d_avg,
          LAG(total_revenue, 7) OVER (
              PARTITION BY region, product_category
              ORDER BY sale_date
          ) AS revenue_7d_ago,
          ROUND(
              (total_revenue - LAG(total_revenue, 7) OVER (
                  PARTITION BY region, product_category
                  ORDER BY sale_date
              )) / NULLIF(LAG(total_revenue, 7) OVER (
                  PARTITION BY region, product_category
                  ORDER BY sale_date
              ), 0) * 100, 2
          ) AS wow_growth_pct
      FROM daily_agg
  )
SELECT * FROM with_running;

-- =====================================================
-- 2. PRODUCT PERFORMANCE (Treemap & Bar Charts)
-- =====================================================
CREATE OR REPLACE VIEW GOLD.PRODUCT_PERFORMANCE AS
SELECT
    p.product_category,
    p.product_subcategory,
    p.product_name,
    COUNT(DISTINCT s.order_id)              AS total_orders,
    SUM(s.quantity)                         AS total_units_sold,
    SUM(s.net_revenue)                      AS revenue,
    SUM(s.profit)                           AS profit,
    AVG(s.unit_price)                       AS avg_selling_price,
    AVG(s.discount_pct)                     AS avg_discount,
    ROUND(SUM(s.profit) / NULLIF(SUM(s.net_revenue), 0) * 100, 2) AS profit_margin_pct,
    RANK() OVER (
          PARTITION BY p.product_category
          ORDER BY SUM(s.net_revenue) DESC
      ) AS category_revenue_rank
FROM SILVER.FACT_SALES s
JOIN SILVER.DIM_PRODUCTS p ON s.product_id = p.product_id
GROUP BY p.product_category, p.product_subcategory, p.product_name;

-- =====================================================
-- 3. WAREHOUSE MONTHLY PERFORMANCE (Heatmap)
-- =====================================================
CREATE OR REPLACE VIEW GOLD.WAREHOUSE_MONTHLY_PERFORMANCE AS
SELECT
    w.warehouse_name,
    EXTRACT(MONTH FROM s.sale_date)         AS month_num,
    TO_CHAR(s.sale_date, 'Mon')             AS month_name,
    COUNT(DISTINCT s.order_id)              AS total_orders,
    SUM(s.quantity)                         AS units_fulfilled,
    SUM(s.net_revenue)                      AS revenue,
    AVG(s.days_to_ship)                     AS avg_days_to_ship,
    ROUND(
          SUM(CASE WHEN s.days_to_ship <= 3 THEN 1 ELSE 0 END)::FLOAT
          / NULLIF(COUNT(*), 0) * 100, 1
      ) AS fulfillment_rate
FROM SILVER.FACT_SALES s
JOIN BRONZE.RAW_WAREHOUSES w ON s.warehouse_id = w.warehouse_id
GROUP BY w.warehouse_name, EXTRACT(MONTH FROM s.sale_date), TO_CHAR(s.sale_date, 'Mon');

-- =====================================================
-- 4. REGIONAL SALES (Choropleth Map)
-- =====================================================
CREATE OR REPLACE VIEW GOLD.REGIONAL_SALES AS
SELECT
    s.state_code,
    s.state_name,
    s.region,
    COUNT(DISTINCT s.order_id)              AS total_orders,
    COUNT(DISTINCT s.customer_id)           AS unique_customers,
    SUM(s.net_revenue)                      AS total_revenue,
    SUM(s.profit)                           AS total_profit,
    AVG(s.net_revenue)                      AS avg_order_value,
    SUM(s.quantity)                         AS total_units
FROM SILVER.FACT_SALES s
GROUP BY s.state_code, s.state_name, s.region;

-- =====================================================
-- 5. CUSTOMER SEGMENTS (Customer Analytics)
-- =====================================================
CREATE OR REPLACE VIEW GOLD.CUSTOMER_ANALYTICS AS
SELECT
    c.customer_id,
    c.customer_name,
    c.segment,
    c.region,
    c.loyalty_tier,
    COUNT(DISTINCT s.order_id)              AS total_orders,
    SUM(s.net_revenue)                      AS lifetime_value,
    AVG(s.net_revenue)                      AS avg_order_value,
    MIN(s.sale_date)                        AS first_purchase,
    MAX(s.sale_date)                        AS last_purchase,
    DATEDIFF('day', MIN(s.sale_date), MAX(s.sale_date)) AS customer_tenure_days,
    COUNT(DISTINCT p.product_category)      AS categories_purchased,
    NTILE(5) OVER (ORDER BY SUM(s.net_revenue) DESC) AS value_quintile
FROM SILVER.FACT_SALES s
JOIN SILVER.DIM_CUSTOMERS c ON s.customer_id = c.customer_id
JOIN SILVER.DIM_PRODUCTS p ON s.product_id = p.product_id
GROUP BY c.customer_id, c.customer_name, c.segment, c.region, c.loyalty_tier;

-- =====================================================
-- 6. KPI SUMMARY (Dashboard Header Cards)
-- =====================================================
CREATE OR REPLACE VIEW GOLD.KPI_SUMMARY AS
SELECT
    COUNT(DISTINCT order_id)                AS total_orders,
    SUM(net_revenue)                        AS total_revenue,
    AVG(net_revenue)                        AS avg_order_value,
    SUM(profit)                             AS total_profit,
    COUNT(DISTINCT customer_id)             AS unique_customers,
    COUNT(DISTINCT product_id)              AS products_sold,
    ROUND(SUM(profit) / NULLIF(SUM(net_revenue), 0) * 100, 2) AS overall_margin_pct
FROM SILVER.FACT_SALES;
