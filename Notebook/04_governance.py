# Databricks notebook source
# ==============================================================================
# 04_governance - Unity Catalog data governance on top of the finished pipeline
#   A) Liquid Clustering : check + OPTIMIZE (clustering is declared in 02 / 03)
#   B) Column masking    : hide PII in dim_customer for people who are not admins
#   C) Row-level filter  : restrict gold_sales_summary to the rows a group may see
#
# Run LAST (after 03_gold). CREATE OR REPLACE TABLE in 03_gold removes policies,
# so if you re-run Gold, re-run this notebook too. It is safe to run many times.
# ==============================================================================

# COMMAND ----------
CATALOG = "workspace"   # same value in all notebooks
SCHEMA = "olist"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# Members of this workspace group see everything; everyone else sees masked / filtered data.
# Demo trick: run once with "admins", then once with "no_such_group" to see the restricted view.
ADMIN_GROUP = "admins"
ANALYST_GROUP = "sp_analysts"   # optional group: may only see orders from state SP

# COMMAND ----------
# MAGIC %md
# MAGIC ## A) Liquid Clustering
# MAGIC Declared when each table is created: `silver_orders` (order_id), `dim_customer` (customer_id)
# MAGIC and `gold_sales_summary` (order_date, customer_state). Pick keys that match how the table is
# MAGIC filtered / joined / merged. OPTIMIZE rewrites files according to those keys.

# COMMAND ----------
for t in ["silver_orders", "dim_customer", "gold_sales_summary"]:
    spark.sql(f"OPTIMIZE {t}")

display(spark.sql("DESCRIBE DETAIL silver_orders")
        .unionByName(spark.sql("DESCRIBE DETAIL dim_customer"))
        .unionByName(spark.sql("DESCRIBE DETAIL gold_sales_summary"))
        .select("name", "clusteringColumns", "numFiles"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## B) Column masking (dim_customer)
# MAGIC A mask is a SQL function: it receives the real value and returns what the reader may see.
# MAGIC - `customer_unique_id` -> SHA-256 hash for non-admins (still joinable, not readable)
# MAGIC - `customer_zip_code_prefix` -> first 2 digits only

# COMMAND ----------
spark.sql(f"""
    CREATE OR REPLACE FUNCTION mask_unique_id(v STRING) RETURNS STRING
    RETURN CASE WHEN is_member('{ADMIN_GROUP}') THEN v ELSE sha2(v, 256) END""")

spark.sql(f"""
    CREATE OR REPLACE FUNCTION mask_zip(v STRING) RETURNS STRING
    RETURN CASE WHEN is_member('{ADMIN_GROUP}') THEN v ELSE concat(substr(v, 1, 2), '***') END""")


def drop_policy(sql):
    try:
        spark.sql(sql)          # fails if no policy exists yet -> ignore, keeps the notebook re-runnable
    except Exception:
        pass


drop_policy("ALTER TABLE dim_customer ALTER COLUMN customer_unique_id DROP MASK")
drop_policy("ALTER TABLE dim_customer ALTER COLUMN customer_zip_code_prefix DROP MASK")
spark.sql("ALTER TABLE dim_customer ALTER COLUMN customer_unique_id SET MASK mask_unique_id")
spark.sql("ALTER TABLE dim_customer ALTER COLUMN customer_zip_code_prefix SET MASK mask_zip")

# COMMAND ----------
# MAGIC %md
# MAGIC ## C) Row-level filter (gold_sales_summary)
# MAGIC A row filter is a SQL function that returns TRUE for rows the reader may see.
# MAGIC Admins see all states; `sp_analysts` only see state SP; everyone else sees nothing.

# COMMAND ----------
spark.sql(f"""
    CREATE OR REPLACE FUNCTION state_filter(state STRING) RETURNS BOOLEAN
    RETURN is_member('{ADMIN_GROUP}') OR (is_member('{ANALYST_GROUP}') AND state = 'SP')""")

drop_policy("ALTER TABLE gold_sales_summary DROP ROW FILTER")
spark.sql("ALTER TABLE gold_sales_summary SET ROW FILTER state_filter ON (customer_state)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify (take screenshots for the README)

# COMMAND ----------
display(spark.sql(f"SELECT current_user() AS me, is_member('{ADMIN_GROUP}') AS is_admin"))

# COMMAND ----------
# masked columns: admin sees real values, others see hash / '01***'
display(spark.sql("""
    SELECT customer_id, customer_unique_id, customer_zip_code_prefix, customer_city
    FROM dim_customer WHERE is_current LIMIT 5"""))

# COMMAND ----------
# row filter: admin sees many states, non-admin sees 0 rows (or only SP for sp_analysts)
display(spark.sql("""
    SELECT customer_state, count(*) AS n_rows, sum(total_sales_revenue) AS revenue
    FROM gold_sales_summary GROUP BY customer_state ORDER BY revenue DESC"""))

# COMMAND ----------
# which masks and row filters are attached (Unity Catalog metadata)
display(spark.sql(f"""
    SELECT table_name, column_name, mask_name
    FROM information_schema.column_masks WHERE table_schema = '{SCHEMA}'"""))
display(spark.sql(f"""
    SELECT table_name, filter_name, target_columns
    FROM information_schema.row_filters WHERE table_schema = '{SCHEMA}'"""))
