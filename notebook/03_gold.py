# Databricks notebook source
# ==============================================================================
# 03_gold - sales mart by day, customer state and product category.
# Reads items/products straight from Bronze (cast inline) to keep the project small.
# Liquid Clustering is declared in the CREATE TABLE statement.
# ==============================================================================

# COMMAND ----------
from pyspark.sql import functions as F

CATALOG = "workspace"   # change to your catalog (same value in all 4 notebooks)
SCHEMA = "olist"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------
orders = spark.table("silver_orders").select("order_id", "customer_id", "order_purchase_timestamp")
cust = spark.table("dim_customer").filter("is_current").select("customer_id", "customer_state")
items = spark.table("bronze_order_items").select(
    "order_id", "product_id",
    F.col("price").cast("decimal(10,2)"), F.col("freight_value").cast("decimal(10,2)"))
products = spark.table("bronze_products").select(
    "product_id", F.coalesce("product_category_name", F.lit("unknown")).alias("product_category_name"))

# Inner joins drop orders that have no items yet (775 in the full dataset).
# countDistinct: one order has several item rows, so COUNT(*) would overcount orders.
gold = (items.join(orders, "order_id").join(cust, "customer_id").join(products, "product_id", "left")
        .groupBy(F.to_date("order_purchase_timestamp").alias("order_date"),
                 "customer_state", "product_category_name")
        .agg(F.sum("price").alias("total_sales_revenue"),
             F.sum("freight_value").alias("total_freight"),
             F.countDistinct("order_id").alias("total_orders")))

gold.createOrReplaceTempView("gold_tmp")
spark.sql("""
    CREATE OR REPLACE TABLE gold_sales_summary
    CLUSTER BY (order_date, customer_state)
    AS SELECT * FROM gold_tmp
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify (take screenshots for the README)

# COMMAND ----------
display(spark.sql("""
    SELECT 'silver_orders' AS t, count(*) AS n FROM silver_orders UNION ALL
    SELECT 'quarantine_orders', count(*) FROM quarantine_orders UNION ALL
    SELECT 'dim_customer (all versions)', count(*) FROM dim_customer UNION ALL
    SELECT 'dim_customer (current)', count(*) FROM dim_customer WHERE is_current"""))

# COMMAND ----------
# orders that received a later version in Bronze (the CDC updates); expected: 1,000
display(spark.sql("""
    SELECT count(*) AS orders_with_updates FROM (
        SELECT order_id FROM bronze_orders
        WHERE order_id RLIKE '^[a-f0-9]{32}$'
        GROUP BY order_id HAVING count(DISTINCT source_updated_at) > 1)"""))

# COMMAND ----------
# why were rows quarantined?
display(spark.sql("""
    SELECT reason, count(*) AS n
    FROM (SELECT explode(reason) AS reason FROM quarantine_orders)
    GROUP BY reason"""))

# COMMAND ----------
# customers with more than one version (SCD2 history)
display(spark.sql("""
    SELECT * FROM dim_customer
    WHERE customer_id IN (SELECT customer_id FROM dim_customer GROUP BY 1 HAVING count(*) > 1)
    ORDER BY customer_id, valid_from
    LIMIT 20"""))

# COMMAND ----------
display(spark.sql("DESCRIBE DETAIL gold_sales_summary"))   # clusteringColumns
display(spark.sql("SELECT * FROM gold_sales_summary ORDER BY total_sales_revenue DESC LIMIT 10"))
