# Databricks notebook source
# ==============================================================================
# 01_bronze - raw data, every column kept as a STRING, plus control columns
#   (_load_dttm, _file_path, _file_size, _file_modification_time).
# orders and customers: Auto Loader (incremental, only new files are read each run)
# order_items and products: one-off load of the source CSV
# All 4 tables get the same control columns (add_control_cols).
# ==============================================================================

# COMMAND ----------
from pyspark.sql import functions as F

CATALOG = "workspace"   # change to your catalog (same value in all 4 notebooks)
SCHEMA = "olist"

SRC = f"/Volumes/{CATALOG}/{SCHEMA}/raw/source"
LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/raw/landing"
CKPT = f"/Volumes/{CATALOG}/{SCHEMA}/raw/_checkpoints"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------
def add_control_cols(df):
    """Control columns for every Bronze table (works for both streaming and batch reads)."""
    return (df
        .withColumn("_load_dttm", F.current_timestamp())
        .withColumn("_file_path", F.col("_metadata.file_path"))
        .withColumn("_file_size", F.col("_metadata.file_size"))                              # bytes
        .withColumn("_file_modification_time", F.col("_metadata.file_modification_time")))  # when the file landed


def ingest(feed):
    raw = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", f"{CKPT}/{feed}/schema")
        .option("cloudFiles.schemaEvolutionMode", "rescue")   # unexpected columns -> _rescued_data
        .option("pathGlobFilter", "*.csv")                    # ignore _SUCCESS files
        .load(f"{LANDING}/{feed}/")
    )
    q = (
        add_control_cols(raw)
        .writeStream
        .option("checkpointLocation", f"{CKPT}/{feed}/checkpoint")
        .trigger(availableNow=True)      # process what is new, then stop
        .toTable(f"bronze_{feed}")
    )
    q.awaitTermination()                 # wait, otherwise Silver may start before Bronze is done


for feed in ["orders", "customers"]:
    ingest(feed)

# COMMAND ----------
for feed, file in [("order_items", "olist_order_items_dataset.csv"),
                   ("products", "olist_products_dataset.csv")]:
    raw = spark.read.option("header", "true").csv(f"{SRC}/{file}")
    (add_control_cols(raw)
          .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
          .saveAsTable(f"bronze_{feed}"))

# COMMAND ----------
for t in ["bronze_orders", "bronze_customers", "bronze_order_items", "bronze_products"]:
    print(t, spark.table(t).count())
