# Databricks notebook source
# ==============================================================================
# 00_mock_data - fake source system that sends CDC / SCD2 batches (PySpark only).
#
# Use the `batch` widget: run with batch = 1, then (after Bronze -> Silver -> Gold) batch = 2.
#   batch 1 : 70% of orders + injected bad rows (null key, malformed key, exact duplicates),
#             all customers
#   batch 2 : the other 30% of orders (new) + 1,000 status updates of batch-1 orders (CDC),
#             1,000 customers who moved to another real address (SCD2)
#
# First time only: set `reset` = yes (drops all tables, landing files and checkpoints),
# then set it back to `no`.
# ==============================================================================

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "workspace"   # change to your catalog (same value in all 4 notebooks)
SCHEMA = "olist"

SRC = f"/Volumes/{CATALOG}/{SCHEMA}/raw/source"         # the 4 uploaded Olist CSVs
LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/raw/landing"    # simulated incoming files
CKPT = f"/Volumes/{CATALOG}/{SCHEMA}/raw/_checkpoints"  # Auto Loader state

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

dbutils.widgets.dropdown("batch", "1", ["1", "2"])
dbutils.widgets.dropdown("reset", "no", ["no", "yes"])
BATCH = dbutils.widgets.get("batch")

# COMMAND ----------
if dbutils.widgets.get("reset") == "yes":
    for t in ["bronze_orders", "bronze_customers", "bronze_order_items", "bronze_products",
              "silver_orders", "dim_customer", "quarantine_orders", "gold_sales_summary",
              # tables from the earlier, bigger version of this project
              "silver_customers", "silver_order_items", "silver_products",
              "quarantine_customers", "dq_metrics"]:
        spark.sql(f"DROP TABLE IF EXISTS {t}")
    dbutils.fs.rm(LANDING, True)
    dbutils.fs.rm(CKPT, True)
    print("reset done")

# COMMAND ----------
BATCH1_TS = "2018-10-01 00:00:00"   # source_updated_at of batch 1
BATCH2_TS = "2018-10-02 00:00:00"   # source_updated_at of batch 2
ADDR = ["customer_zip_code_prefix", "customer_city", "customer_state"]


def read_src(name):
    # no inferSchema: everything stays string (keeps zip codes like "01151")
    return spark.read.option("header", "true").csv(f"{SRC}/{name}")


def write_feed(df, feed):
    (df.coalesce(1).write.mode("overwrite").option("header", "true")
       .csv(f"{LANDING}/{feed}/batch{BATCH}"))


def pick(df, n, salt, key):
    """Deterministic 'random' sample of n rows (hash order, so every run gives the same files)."""
    return df.orderBy(F.xxhash64(F.col(key), F.lit(salt))).limit(n)


def in_batch1():
    """Deterministic 70/30 split of orders by key hash."""
    return F.pmod(F.xxhash64("order_id"), F.lit(100)) < 70


orders = read_src("olist_orders_dataset.csv")
customers = read_src("olist_customers_dataset.csv")

# COMMAND ----------
if BATCH == "1":
    b1 = orders.filter(in_batch1()).withColumn("source_updated_at", F.lit(BATCH1_TS))

    # inject bad rows so the DQ step has something to catch (raw Olist data is clean)
    null_key = pick(b1, 10, 1, "order_id").withColumn("order_id", F.lit(None).cast("string"))
    bad_fmt = pick(b1, 10, 2, "order_id").withColumn(
        "order_id", F.concat(F.lit("BAD-"), F.substring("order_id", 1, 8)))
    dups = pick(b1, 10, 3, "order_id")                      # exact copies of existing rows
    write_feed(b1.unionByName(null_key).unionByName(bad_fmt).unionByName(dups), "orders")

    write_feed(customers.withColumn("source_updated_at", F.lit(BATCH1_TS)), "customers")

else:
    # CDC: new orders + status updates for orders that were still pending in batch 1
    new_orders = orders.filter(~in_batch1()).withColumn("source_updated_at", F.lit(BATCH2_TS))
    pending = orders.filter(in_batch1() & (F.col("order_status") != "delivered"))
    updates = (pick(pending, 1000, 6, "order_id")
               .withColumn("order_status", F.lit("delivered"))
               .withColumn("order_delivered_customer_date", F.col("order_estimated_delivery_date"))
               .withColumn("source_updated_at", F.lit(BATCH2_TS)))
    write_feed(new_orders.unionByName(updates), "orders")

    # SCD2: 1,000 customers move; the new address is drawn from real addresses in the data
    movers = (pick(customers, 1000, 7, "customer_id")
              .withColumn("_rn", F.row_number().over(Window.orderBy(F.xxhash64("customer_id", F.lit(7))))))
    pool = (customers.select(*ADDR).distinct()
            .withColumn("_rn", F.row_number().over(Window.orderBy(F.xxhash64(*ADDR, F.lit(8)))))
            .filter("_rn <= 1000")
            .select("_rn", *[F.col(c).alias(f"new_{c}") for c in ADDR]))
    # (a mover can occasionally draw the old address again -> no change -> no new SCD2 version)
    moved = (movers.join(pool, "_rn")
             .select("customer_id", "customer_unique_id", *[F.col(f"new_{c}").alias(c) for c in ADDR])
             .withColumn("source_updated_at", F.lit(BATCH2_TS)))
    write_feed(moved, "customers")

print(f"batch {BATCH} written to {LANDING}")
