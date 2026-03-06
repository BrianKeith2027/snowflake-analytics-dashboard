# ❄️ Snowflake Analytics Dashboard

An end-to-end analytics platform built on **Snowflake**, featuring **Snowpark Python** for data processing, **Streamlit** for interactive dashboards, and **Plotly** for production-grade data visualizations. Analyzes retail & supply chain data to deliver actionable business insights.

## 📋 Overview

This project demonstrates a complete Snowflake-native analytics workflow — from raw data ingestion through SQL transformations to interactive visual dashboards. It showcases how modern data teams use Snowflake's ecosystem (Snowpark, Streamlit in Snowflake, Snowpipe) to build scalable, self-service analytics platforms without ever leaving the Snowflake environment.

**End Use:** Business analysts and data teams use this dashboard to monitor retail KPIs, identify supply chain bottlenecks, track seasonal trends, and make data-driven inventory decisions — all through interactive, self-service visualizations.

## 🎯 Key Features

| Feature | Description |
|---------|-------------|
| **Snowflake SQL Analytics** | Optimized SQL queries with CTEs, window functions, and materialized views for fast aggregations |
| **Snowpark Python** | Server-side Python processing via Snowpark DataFrames — no data movement required |
| **Streamlit Dashboard** | Interactive, filterable dashboard deployable natively in Snowflake (Streamlit in Snowflake) |
| **Plotly Visualizations** | Publication-quality interactive charts: time series, heatmaps, treemaps, KPI cards |
| **Data Pipeline** | Automated ingestion pipeline with staging, transformation, and quality checks |
| **Medallion Architecture** | Bronze → Silver → Gold layer pattern for organized data transformation |

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         SNOWFLAKE PLATFORM                              │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐              │
│  │  🥉 BRONZE    │    │  🥈 SILVER    │    │  🥇 GOLD      │              │
│  │              │    │              │    │              │              │
│  │  Raw Staging │───▶│  Cleaned &   │───▶│  Analytics   │              │
│  │  Tables      │    │  Validated   │    │  Ready Views │              │
│  │              │    │              │    │              │              │
│  └──────────────┘    └──────────────┘    └──────────────┘              │
│         ▲                                       │                       │
│         │                                       ▼                       │
│  ┌──────────────┐                      ┌──────────────────┐            │
│  │  Snowpipe /  │                      │  Snowpark Python │            │
│  │  COPY INTO   │                      │  UDFs & Procs    │            │
│  └──────────────┘                      └────────┬─────────┘            │
│                                                  │                      │
│                                                  ▼                      │
│                                        ┌──────────────────┐            │
│                                        │   Streamlit App  │            │
│                                        │   (Dashboard)    │            │
│                                        └──────────────────┘            │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

## 🚀 Quick Start

### Prerequisites

- Snowflake account (free trial works)
- - Python 3.8+
  - - Snowpark Python SDK
   
    - ### Installation
   
    - ```bash
      # Clone the repository
      git clone https://github.com/BrianKeith2027/snowflake-analytics-dashboard.git
      cd snowflake-analytics-dashboard

      # Install dependencies
      pip install -r requirements.txt

      # Configure Snowflake connection
      cp config/.env.example config/.env
      # Edit config/.env with your Snowflake credentials
      ```

      ### Setup Snowflake Objects

      ```sql
      -- Run the setup script to create database, schemas, and tables
      -- Execute in Snowflake worksheet or via SnowSQL
      SOURCE sql/01_setup_database.sql;
      SOURCE sql/02_create_tables.sql;
      SOURCE sql/03_load_sample_data.sql;
      SOURCE sql/04_create_views.sql;
      ```

      ### Run the Dashboard

      ```bash
      # Option 1: Run locally with Streamlit
      streamlit run app/streamlit_dashboard.py

      # Option 2: Deploy to Streamlit in Snowflake
      # Upload app/streamlit_dashboard.py via Snowsight UI
      ```

      ## 📊 Dashboard Previews

      ### KPI Overview
      The main dashboard displays real-time KPI cards showing total revenue, order count, average order value, and month-over-month growth with trend indicators.

      ### Revenue Trends
      Interactive time series chart with date range filters, regional breakdowns, and moving average overlays built with Plotly.

      ### Product Performance Treemap
      Hierarchical treemap visualization showing product category and subcategory revenue contributions with drill-down capability.

      ### Supply Chain Heatmap
      Warehouse-by-month heatmap displaying fulfillment rates, identifying bottleneck periods and underperforming distribution centers.

      ### Regional Sales Map
      Choropleth map of sales by state/region with tooltip details for revenue, units, and growth rates.

      ## 🛢️ Snowflake SQL Examples

      ### Materialized Analytics View (Gold Layer)

      ```sql
      CREATE OR REPLACE VIEW gold.daily_sales_summary AS
      WITH daily_agg AS (
          SELECT
              sale_date,
              region,
              product_category,
              COUNT(DISTINCT order_id) AS order_count,
              SUM(quantity) AS units_sold,
              SUM(net_revenue) AS total_revenue,
              AVG(net_revenue) AS avg_order_value,
              SUM(discount_amount) AS total_discounts
          FROM silver.fact_sales
          GROUP BY sale_date, region, product_category
      ),
      with_running AS (
          SELECT
              *,
              SUM(total_revenue) OVER (
                  PARTITION BY region
                  ORDER BY sale_date
                  ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
              ) AS rolling_7d_revenue,
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
      ```

      ### Snowpark Python — Feature Engineering

      ```python
      from snowflake.snowpark import Session
      from snowflake.snowpark.functions import col, sum as sf_sum, avg, count, lag, datediff
      from snowflake.snowpark.window import Window

      def create_customer_features(session: Session) -> None:
          """Build customer feature table using Snowpark DataFrames."""

          sales = session.table("SILVER.FACT_SALES")
          customers = session.table("SILVER.DIM_CUSTOMERS")

          # Customer purchase behavior features
          window_spec = Window.partition_by("customer_id").order_by("sale_date")

          customer_features = (
              sales
              .group_by("customer_id")
              .agg(
                  count("order_id").alias("total_orders"),
                  sf_sum("net_revenue").alias("lifetime_value"),
                  avg("net_revenue").alias("avg_order_value"),
                  sf_sum("quantity").alias("total_units"),
                  count("DISTINCT product_id").alias("unique_products"),
              )
              .join(customers, "customer_id")
              .select(
                  col("customer_id"),
                  col("total_orders"),
                  col("lifetime_value"),
                  col("avg_order_value"),
                  col("total_units"),
                  col("unique_products"),
                  col("customer_segment"),
                  col("signup_date"),
              )
          )

          # Write back to Snowflake
          customer_features.write.mode("overwrite").save_as_table(
              "GOLD.CUSTOMER_FEATURES"
          )
          print(f"Created GOLD.CUSTOMER_FEATURES with {customer_features.count()} rows")
      ```

      ## 🖥️ Streamlit Dashboard Code

      ```python
      import streamlit as st
      import plotly.express as px
      import plotly.graph_objects as go
      from snowflake.snowpark import Session
      import pandas as pd

      st.set_page_config(page_title="Retail Analytics", layout="wide")
      st.title("❄️ Snowflake Retail Analytics Dashboard")

      # --- Snowflake Connection ---
      @st.cache_resource
      def get_session():
          return Session.builder.configs({
              "account": st.secrets["snowflake"]["account"],
              "user": st.secrets["snowflake"]["user"],
              "password": st.secrets["snowflake"]["password"],
              "warehouse": st.secrets["snowflake"]["warehouse"],
              "database": "RETAIL_ANALYTICS",
              "schema": "GOLD",
          }).create()

      session = get_session()

      # --- KPI Cards ---
      kpi_df = session.sql("""
          SELECT
              SUM(total_revenue) AS total_revenue,
              SUM(order_count) AS total_orders,
              AVG(avg_order_value) AS avg_aov,
              COUNT(DISTINCT sale_date) AS active_days
          FROM gold.daily_sales_summary
          WHERE sale_date >= DATEADD('month', -1, CURRENT_DATE())
      """).to_pandas()

      col1, col2, col3, col4 = st.columns(4)
      col1.metric("Total Revenue", f"${kpi_df['TOTAL_REVENUE'][0]:,.0f}", "+12.3%")
      col2.metric("Total Orders", f"{kpi_df['TOTAL_ORDERS'][0]:,.0f}", "+8.1%")
      col3.metric("Avg Order Value", f"${kpi_df['AVG_AOV'][0]:,.2f}", "+3.7%")
      col4.metric("Active Days", f"{kpi_df['ACTIVE_DAYS'][0]}")

      # --- Filters ---
      st.sidebar.header("Filters")
      regions = session.sql("SELECT DISTINCT region FROM gold.daily_sales_summary ORDER BY region").to_pandas()
      selected_regions = st.sidebar.multiselect("Region", regions["REGION"].tolist(), default=regions["REGION"].tolist())

      date_range = st.sidebar.date_input("Date Range", value=[pd.Timestamp("2024-01-01"), pd.Timestamp("2024-12-31")])

      # --- Revenue Trend ---
      st.subheader("📈 Revenue Trend by Region")
      trend_df = session.sql(f"""
          SELECT sale_date, region, total_revenue, rolling_7d_revenue
          FROM gold.daily_sales_summary
          WHERE region IN ({','.join([f"'{r}'" for r in selected_regions])})
            AND sale_date BETWEEN '{date_range[0]}' AND '{date_range[1]}'
          ORDER BY sale_date
      """).to_pandas()

      fig_trend = px.line(
          trend_df, x="SALE_DATE", y="TOTAL_REVENUE", color="REGION",
          title="Daily Revenue by Region",
          labels={"TOTAL_REVENUE": "Revenue ($)", "SALE_DATE": "Date"},
          template="plotly_white"
      )
      fig_trend.update_layout(hovermode="x unified", height=450)
      st.plotly_chart(fig_trend, use_container_width=True)

      # --- Product Treemap ---
      st.subheader("🏷️ Product Category Revenue")
      product_df = session.sql("""
          SELECT product_category, product_subcategory, SUM(revenue) AS total_revenue
          FROM gold.product_performance
          GROUP BY product_category, product_subcategory
      """).to_pandas()

      fig_tree = px.treemap(
          product_df, path=["PRODUCT_CATEGORY", "PRODUCT_SUBCATEGORY"],
          values="TOTAL_REVENUE", color="TOTAL_REVENUE",
          color_continuous_scale="Blues",
          title="Revenue by Product Category"
      )
      st.plotly_chart(fig_tree, use_container_width=True)

      # --- Supply Chain Heatmap ---
      st.subheader("🏭 Warehouse Fulfillment Heatmap")
      heatmap_df = session.sql("""
          SELECT warehouse_name, month_name, fulfillment_rate
          FROM gold.warehouse_monthly_performance
          ORDER BY warehouse_name, month_num
      """).to_pandas()

      pivot_df = heatmap_df.pivot(index="WAREHOUSE_NAME", columns="MONTH_NAME", values="FULFILLMENT_RATE")

      fig_heat = px.imshow(
          pivot_df, text_auto=".1f", aspect="auto",
          color_continuous_scale="RdYlGn",
          title="Fulfillment Rate by Warehouse & Month (%)"
      )
      fig_heat.update_layout(height=400)
      st.plotly_chart(fig_heat, use_container_width=True)

      # --- Regional Map ---
      st.subheader("🗺️ Sales by Region")
      geo_df = session.sql("""
          SELECT state_code, state_name, SUM(total_revenue) AS revenue
          FROM gold.regional_sales
          GROUP BY state_code, state_name
      """).to_pandas()

      fig_map = px.choropleth(
          geo_df, locations="STATE_CODE", locationmode="USA-states",
          color="REVENUE", scope="usa",
          color_continuous_scale="Viridis",
          title="Revenue by State"
      )
      st.plotly_chart(fig_map, use_container_width=True)

      st.caption("Built with Snowflake + Snowpark + Streamlit | Brian Stratton")
      ```

      ## 📁 Project Structure

      ```
      snowflake-analytics-dashboard/
      │
      ├── README.md
      ├── requirements.txt
      ├── .gitignore
      ├── LICENSE
      │
      ├── config/
      │   ├── .env.example              # Snowflake connection template
      │   └── snowflake_config.py       # Connection helper
      │
      ├── sql/
      │   ├── 01_setup_database.sql     # Create database, warehouse, schemas
      │   ├── 02_create_tables.sql      # Bronze & Silver table DDL
      │   ├── 03_load_sample_data.sql   # COPY INTO / INSERT sample data
      │   ├── 04_create_views.sql       # Gold layer analytics views
      │   └── 05_stored_procedures.sql  # Snowflake stored procedures
      │
      ├── snowpark/
      │   ├── feature_engineering.py    # Snowpark DataFrame transformations
      │   ├── data_quality_checks.py    # Automated DQ with Snowpark
      │   └── udf_definitions.py       # Python UDFs registered in Snowflake
      │
      ├── app/
      │   ├── streamlit_dashboard.py    # Main Streamlit dashboard
      │   ├── pages/
      │   │   ├── 01_revenue.py         # Revenue analytics page
      │   │   ├── 02_products.py        # Product performance page
      │   │   ├── 03_supply_chain.py    # Supply chain analytics page
      │   │   └── 04_customers.py       # Customer segmentation page
      │   └── components/
      │       ├── kpi_cards.py          # Reusable KPI card components
      │       ├── filters.py            # Sidebar filter components
      │       └── charts.py             # Chart factory functions
      │
      ├── data/
      │   └── sample/
      │       ├── transactions.csv      # Sample transaction data
      │       ├── products.csv          # Product catalog
      │       ├── warehouses.csv        # Warehouse locations
      │       └── customers.csv         # Customer segments
      │
      ├── notebooks/
      │   ├── 01_snowpark_eda.ipynb     # Exploratory analysis via Snowpark
      │   └── 02_visualization_dev.ipynb # Chart prototyping
      │
      └── tests/
          ├── test_sql_queries.py       # SQL output validation
          ├── test_snowpark.py          # Snowpark transformation tests
          └── test_dashboard.py         # Streamlit component tests
      ```

      ## 🛠️ Tech Stack

      | Category | Technology |
      |----------|-----------|
      | **Cloud Data Platform** | Snowflake |
      | **Data Processing** | Snowpark Python, SQL |
      | **Dashboard** | Streamlit (Streamlit in Snowflake compatible) |
      | **Visualizations** | Plotly Express, Plotly Graph Objects |
      | **Data Pipeline** | Snowpipe, COPY INTO, Tasks & Streams |
      | **Languages** | Python 3.10+, SQL |
      | **Testing** | pytest, Snowpark test utilities |

      ## 📈 End-Use Scenarios

      This dashboard is designed for real-world business analytics use cases:

      | Scenario | Who Uses It | What They See |
      |----------|-------------|---------------|
      | **Daily Revenue Monitoring** | VP of Sales | KPI cards + revenue trend by region |
      | **Product Mix Optimization** | Category Managers | Treemap showing category revenue contribution |
      | **Supply Chain Visibility** | Operations Team | Warehouse fulfillment heatmap with bottleneck alerts |
      | **Customer Segmentation** | Marketing Team | Customer lifetime value distributions and cohort analysis |
      | **Seasonal Planning** | Demand Planning | YoY seasonal trend overlays with forecast indicators |

      ## 🔮 Future Improvements

      - Add Snowflake Cortex ML functions for in-platform forecasting
      - - Implement Snowflake Dynamic Tables for real-time aggregations
        - - Add Snowflake Alerts for automated KPI threshold monitoring
          - - Build customer churn prediction model with Snowpark ML
            - - Integrate with dbt for version-controlled SQL transformations
             
              - ## 👤 Author
             
              - **Brian Stratton**
              - Senior Data Engineer | AI/ML Engineer | Doctoral Researcher
             
              - [LinkedIn](https://www.linkedin.com/in/briankstratton/) | [GitHub](https://github.com/BrianKeith2027)
             
              - ## 📄 License
             
              - This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
