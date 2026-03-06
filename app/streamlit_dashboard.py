"""
Snowflake Retail Analytics Dashboard
=====================================
Author: Brian Stratton
Description: Interactive Streamlit dashboard connected to Snowflake
             via Snowpark. Displays KPIs, revenue trends, product
                          performance, supply chain heatmaps, and regional maps.

                          Usage:
                              streamlit run app/streamlit_dashboard.py
                              """

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from snowflake.snowpark import Session
import pandas as pd
import os
from dotenv import load_dotenv

# ── Page Configuration ──────────────────────────────────────
st.set_page_config(
      page_title="Retail Analytics Dashboard",
      page_icon="snowflake",
      layout="wide",
      initial_sidebar_state="expanded",
)

st.title("Snowflake Retail Analytics Dashboard")
st.markdown("Real-time retail & supply chain insights powered by Snowflake + Snowpark")


# ── Snowflake Connection ────────────────────────────────────
@st.cache_resource
def get_session() -> Session:
      """Establish a cached Snowpark session."""
      load_dotenv("config/.env")
      return Session.builder.configs({
          "account": os.getenv("SNOWFLAKE_ACCOUNT", st.secrets.get("snowflake", {}).get("account", "")),
          "user": os.getenv("SNOWFLAKE_USER", st.secrets.get("snowflake", {}).get("user", "")),
          "password": os.getenv("SNOWFLAKE_PASSWORD", st.secrets.get("snowflake", {}).get("password", "")),
          "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "ANALYTICS_WH"),
          "database": "RETAIL_ANALYTICS",
          "schema": "GOLD",
      }).create()


@st.cache_data(ttl=600)
def run_query(query: str) -> pd.DataFrame:
      """Execute a SQL query and return a Pandas DataFrame (cached 10 min)."""
      session = get_session()
      return session.sql(query).to_pandas()


# ── Sidebar Filters ─────────────────────────────────────────
st.sidebar.header("Dashboard Filters")

regions_df = run_query(
      "SELECT DISTINCT region FROM GOLD.DAILY_SALES_SUMMARY ORDER BY region"
)
all_regions = regions_df["REGION"].tolist()
selected_regions = st.sidebar.multiselect(
      "Region", all_regions, default=all_regions
)

categories_df = run_query(
      "SELECT DISTINCT product_category FROM GOLD.DAILY_SALES_SUMMARY ORDER BY product_category"
)
all_categories = categories_df["PRODUCT_CATEGORY"].tolist()
selected_categories = st.sidebar.multiselect(
      "Product Category", all_categories, default=all_categories
)

date_range = st.sidebar.date_input(
      "Date Range",
      value=[pd.Timestamp("2024-01-01"), pd.Timestamp("2024-12-31")],
)
start_date, end_date = date_range[0], date_range[1]

region_filter = ",".join([f"'{r}'" for r in selected_regions])
category_filter = ",".join([f"'{c}'" for c in selected_categories])


# ── KPI Cards ───────────────────────────────────────────────
st.subheader("Key Performance Indicators")

kpi_df = run_query(f"""
    SELECT
            SUM(total_revenue)                AS total_revenue,
                    SUM(order_count)                  AS total_orders,
                            AVG(avg_order_value)              AS avg_aov,
                                    SUM(total_profit)                 AS total_profit,
                                            SUM(units_sold)                   AS total_units,
                                                    COUNT(DISTINCT sale_date)         AS active_days
                                                        FROM GOLD.DAILY_SALES_SUMMARY
                                                            WHERE region IN ({region_filter})
                                                                  AND product_category IN ({category_filter})
                                                                        AND sale_date BETWEEN '{start_date}' AND '{end_date}'
                                                                        """)

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total Revenue",  f"${kpi_df['TOTAL_REVENUE'][0]:,.0f}")
c2.metric("Total Orders",   f"{kpi_df['TOTAL_ORDERS'][0]:,.0f}")
c3.metric("Avg Order Value", f"${kpi_df['AVG_AOV'][0]:,.2f}")
c4.metric("Total Profit",   f"${kpi_df['TOTAL_PROFIT'][0]:,.0f}")
c5.metric("Units Sold",     f"{kpi_df['TOTAL_UNITS'][0]:,.0f}")

st.divider()

# ── Revenue Trend Line Chart ────────────────────────────────
st.subheader("Revenue Trend by Region")

trend_df = run_query(f"""
    SELECT sale_date, region, SUM(total_revenue) AS revenue,
               SUM(rolling_7d_revenue) AS rolling_7d
                   FROM GOLD.DAILY_SALES_SUMMARY
                       WHERE region IN ({region_filter})
                             AND product_category IN ({category_filter})
                                   AND sale_date BETWEEN '{start_date}' AND '{end_date}'
                                       GROUP BY sale_date, region
                                           ORDER BY sale_date
                                           """)

fig_trend = px.line(
      trend_df,
      x="SALE_DATE",
      y="REVENUE",
      color="REGION",
      title="Daily Revenue by Region",
      labels={"REVENUE": "Revenue ($)", "SALE_DATE": "Date"},
      template="plotly_white",
)
fig_trend.update_layout(hovermode="x unified", height=450, legend_title="Region")
st.plotly_chart(fig_trend, use_container_width=True)

# ── Product Treemap & Bar Chart ─────────────────────────────
col_left, col_right = st.columns(2)

with col_left:
      st.subheader("Product Category Revenue")
      product_df = run_query("""
          SELECT product_category, product_subcategory,
                 SUM(revenue) AS total_revenue
          FROM GOLD.PRODUCT_PERFORMANCE
          GROUP BY product_category, product_subcategory
      """)

    fig_tree = px.treemap(
              product_df,
              path=["PRODUCT_CATEGORY", "PRODUCT_SUBCATEGORY"],
              values="TOTAL_REVENUE",
              color="TOTAL_REVENUE",
              color_continuous_scale="Blues",
              title="Revenue by Product Hierarchy",
    )
    fig_tree.update_layout(height=450)
    st.plotly_chart(fig_tree, use_container_width=True)

with col_right:
      st.subheader("Top Products by Revenue")
      top_products = run_query("""
          SELECT product_name, product_category, revenue, profit_margin_pct
          FROM GOLD.PRODUCT_PERFORMANCE
          ORDER BY revenue DESC
          LIMIT 15
      """)

    fig_bar = px.bar(
              top_products,
              x="REVENUE",
              y="PRODUCT_NAME",
              color="PRODUCT_CATEGORY",
              orientation="h",
              title="Top 15 Products",
              labels={"REVENUE": "Revenue ($)", "PRODUCT_NAME": "Product"},
              template="plotly_white",
    )
    fig_bar.update_layout(height=450, yaxis={"autorange": "reversed"})
    st.plotly_chart(fig_bar, use_container_width=True)

st.divider()

# ── Warehouse Fulfillment Heatmap ───────────────────────────
st.subheader("Warehouse Fulfillment Rate Heatmap")

heatmap_df = run_query("""
    SELECT warehouse_name, month_name, month_num, fulfillment_rate
        FROM GOLD.WAREHOUSE_MONTHLY_PERFORMANCE
            ORDER BY warehouse_name, month_num
            """)

month_order = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
pivot_df = heatmap_df.pivot(
      index="WAREHOUSE_NAME", columns="MONTH_NAME", values="FULFILLMENT_RATE"
)
pivot_df = pivot_df.reindex(columns=[m for m in month_order if m in pivot_df.columns])

fig_heat = px.imshow(
      pivot_df,
      text_auto=".1f",
      aspect="auto",
      color_continuous_scale="RdYlGn",
      title="Fulfillment Rate by Warehouse & Month (%)",
      labels={"color": "Rate (%)"},
)
fig_heat.update_layout(height=400)
st.plotly_chart(fig_heat, use_container_width=True)

# ── Regional Sales Choropleth ───────────────────────────────
st.subheader("Sales by State")

geo_df = run_query("""
    SELECT state_code, state_name, total_revenue, total_orders, unique_customers
        FROM GOLD.REGIONAL_SALES
        """)

fig_map = px.choropleth(
      geo_df,
      locations="STATE_CODE",
      locationmode="USA-states",
      color="TOTAL_REVENUE",
      scope="usa",
      color_continuous_scale="Viridis",
      title="Revenue by State",
      hover_data=["STATE_NAME", "TOTAL_ORDERS", "UNIQUE_CUSTOMERS"],
      labels={"TOTAL_REVENUE": "Revenue ($)"},
)
fig_map.update_layout(height=500, geo=dict(bgcolor="rgba(0,0,0,0)"))
st.plotly_chart(fig_map, use_container_width=True)

# ── Footer ──────────────────────────────────────────────────
st.divider()
st.caption(
      "Built with Snowflake + Snowpark + Streamlit + Plotly | "
      "Brian Stratton | github.com/BrianKeith2027"
)
