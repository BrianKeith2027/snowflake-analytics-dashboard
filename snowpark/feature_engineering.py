"""
Snowpark Feature Engineering Module
====================================
Author: Brian Stratton
Description: Server-side data transformations using Snowpark Python
             DataFrames. Runs inside Snowflake's compute — no data
                          movement required.
                          """

from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col, sum as sf_sum, avg, count, min as sf_min,
    max as sf_max, datediff, lit, when, round as sf_round,
    current_timestamp, ntile
)
from snowflake.snowpark.window import Window
from snowflake.snowpark.types import FloatType, IntegerType
import os
from dotenv import load_dotenv


def get_snowpark_session() -> Session:
      """Create a Snowpark session from environment variables."""
      load_dotenv("config/.env")

    connection_params = {
              "account": os.getenv("SNOWFLAKE_ACCOUNT"),
              "user": os.getenv("SNOWFLAKE_USER"),
              "password": os.getenv("SNOWFLAKE_PASSWORD"),
              "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "ANALYTICS_WH"),
              "database": os.getenv("SNOWFLAKE_DATABASE", "RETAIL_ANALYTICS"),
              "schema": os.getenv("SNOWFLAKE_SCHEMA", "GOLD"),
    }
    return Session.builder.configs(connection_params).create()


def transform_bronze_to_silver(session: Session) -> None:
      """
          Transform raw Bronze layer data into cleaned Silver tables.
              Handles type casting, date parsing, null handling, and
                  calculated field generation.
                      """
      print("Starting Bronze -> Silver transformation...")

    raw_txn = session.table("BRONZE.RAW_TRANSACTIONS")

    silver_sales = (
              raw_txn
              .with_column("sale_date", col("order_date").cast("DATE"))
              .with_column("ship_date_parsed", col("ship_date").cast("DATE"))
              .with_column("discount_pct_clean",
                                                 when(col("discount_pct").is_null(), lit(0.0))
                                                 .otherwise(col("discount_pct")))
              .with_column("gross_revenue", col("quantity") * col("unit_price"))
              .with_column("discount_amount",
                                                 col("quantity") * col("unit_price") * col("discount_pct_clean"))
              .with_column("net_revenue",
                                                 col("quantity") * col("unit_price") * (lit(1) - col("discount_pct_clean")))
              .with_column("days_to_ship",
                                                 datediff("day", col("sale_date"), col("ship_date_parsed")))
              .with_column("sale_year", col("sale_date").substr(1, 4).cast(IntegerType()))
              .with_column("sale_month", col("sale_date").substr(6, 2).cast(IntegerType()))
              .with_column("sale_quarter",
                                                 sf_round((col("sale_month") - lit(1)) / lit(3) + lit(1), 0))
              .select(
                            col("order_id"),
                            col("sale_date"),
                            col("ship_date_parsed").alias("ship_date"),
                            col("customer_id"),
                            col("product_id"),
                            col("region"),
                            col("state"),
                            col("quantity"),
                            col("unit_price"),
                            col("discount_pct_clean").alias("discount_pct"),
                            col("gross_revenue"),
                            col("discount_amount"),
                            col("net_revenue"),
                            col("profit"),
                            col("ship_mode"),
                            col("days_to_ship"),
                            col("sale_year"),
                            col("sale_month"),
                            col("sale_quarter"),
              )
    )

    silver_sales.write.mode("overwrite").save_as_table("SILVER.FACT_SALES")
    row_count = session.table("SILVER.FACT_SALES").count()
    print(f"  SILVER.FACT_SALES created with {row_count:,} rows")


def create_customer_features(session: Session) -> None:
      """
          Build customer feature table using Snowpark DataFrames.
              Aggregates purchase behavior for customer segmentation
                  and lifetime value analysis.
                      """
      print("Building customer features...")

    sales = session.table("SILVER.FACT_SALES")
    customers = session.table("SILVER.DIM_CUSTOMERS")

    customer_features = (
              sales
              .group_by("customer_id")
              .agg(
                            count("order_id").alias("total_orders"),
                            sf_sum("net_revenue").alias("lifetime_value"),
                            avg("net_revenue").alias("avg_order_value"),
                            sf_sum("quantity").alias("total_units"),
                            sf_min("sale_date").alias("first_purchase"),
                            sf_max("sale_date").alias("last_purchase"),
              )
              .join(customers, "customer_id")
              .with_column("tenure_days",
                                                 datediff("day", col("first_purchase"), col("last_purchase")))
              .with_column("purchase_frequency",
                                                 when(col("tenure_days") > lit(0),
                                                                                 col("total_orders") / col("tenure_days") * lit(30))
                                                 .otherwise(lit(0)))
              .select(
                            col("customer_id"),
                            col("customer_name"),
                            col("segment"),
                            col("region"),
                            col("loyalty_tier"),
                            col("total_orders"),
                            col("lifetime_value"),
                            col("avg_order_value"),
                            col("total_units"),
                            col("first_purchase"),
                            col("last_purchase"),
                            col("tenure_days"),
                            col("purchase_frequency"),
              )
    )

    customer_features.write.mode("overwrite").save_as_table("GOLD.CUSTOMER_FEATURES")
    row_count = session.table("GOLD.CUSTOMER_FEATURES").count()
    print(f"  GOLD.CUSTOMER_FEATURES created with {row_count:,} rows")


def create_product_features(session: Session) -> None:
      """
          Build product analytics features for dashboard consumption.
              """
      print("Building product features...")

    sales = session.table("SILVER.FACT_SALES")
    products = session.table("SILVER.DIM_PRODUCTS")

    product_features = (
              sales
              .group_by("product_id")
              .agg(
                            count("order_id").alias("total_orders"),
                            sf_sum("quantity").alias("total_units_sold"),
                            sf_sum("net_revenue").alias("total_revenue"),
                            sf_sum("profit").alias("total_profit"),
                            avg("unit_price").alias("avg_selling_price"),
                            avg("discount_pct").alias("avg_discount_rate"),
              )
              .join(products, "product_id")
              .with_column("profit_margin",
                                                 when(col("total_revenue") > lit(0),
                                                                                 sf_round(col("total_profit") / col("total_revenue") * lit(100), 2))
                                                 .otherwise(lit(0)))
              .select(
                            col("product_id"),
                            col("product_name"),
                            col("product_category"),
                            col("product_subcategory"),
                            col("total_orders"),
                            col("total_units_sold"),
                            col("total_revenue"),
                            col("total_profit"),
                            col("profit_margin"),
                            col("avg_selling_price"),
                            col("avg_discount_rate"),
              )
    )

    product_features.write.mode("overwrite").save_as_table("GOLD.PRODUCT_FEATURES")
    row_count = session.table("GOLD.PRODUCT_FEATURES").count()
    print(f"  GOLD.PRODUCT_FEATURES created with {row_count:,} rows")


def run_all_transformations() -> None:
      """Execute the full feature engineering pipeline."""
      session = get_snowpark_session()
      print("=" * 60)
      print("Snowpark Feature Engineering Pipeline")
      print("=" * 60)

    try:
              transform_bronze_to_silver(session)
              create_customer_features(session)
              create_product_features(session)
              print("\nAll transformations completed successfully!")
except Exception as e:
          print(f"\nError during transformation: {e}")
          raise
finally:
          session.close()


if __name__ == "__main__":
      run_all_transformations()
