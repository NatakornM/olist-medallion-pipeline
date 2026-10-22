# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# ==============================================================================
# 02_silver
#   A) orders    : DQ (flags -> melt -> `reason`) -> quarantine, dedup, CDC MERGE
#   B) customers : SCD Type 2 dimension (dim_customer)
# ==============================================================================

# COMMAND ----------

from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

CATALOG = "workspace"   # change to your catalog (same value in all 4 notebooks)
SCHEMA = "olist"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

HEX32 = r"^[a-f0-9]{32}$"
TS_FMT = "yyyy-MM-dd HH:mm:ss"
VALID_STATUS = ["created", "approved", "invoiced", "processing",
                "shipped", "delivered", "unavailable", "canceled"]
TS_COLS = ["order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
           "order_delivered_customer_date", "order_estimated_delivery_date"]
ORDER_COLS = ["order_id", "customer_id", "order_status", *TS_COLS, "source_updated_at"]


def try_ts(col):
    return F.expr(f"try_to_timestamp({col}, '{TS_FMT}')")   # NULL instead of an error if invalid

# COMMAND ----------

# MAGIC %md
# MAGIC ## A) Orders: data quality

# COMMAND ----------

# _sk = hash of the row's content. NULLs are replaced by a marker because xxhash64 skips NULLs.
bronze = (spark.table("bronze_orders")
          .withColumn("_sk", F.xxhash64(*[F.coalesce(F.col(c), F.lit("<NULL>")) for c in ORDER_COLS])))

# One boolean flag per rule: TRUE = the row is bad
flagged = (bronze
    .withColumn("_is_order_id_null", F.col("order_id").isNull())
    .withColumn("_is_order_id_invalid", F.col("order_id").isNotNull() & ~F.col("order_id").rlike(HEX32))
    .withColumn("_is_customer_id_null", F.col("customer_id").isNull())
    .withColumn("_is_purchase_ts_invalid", try_ts("order_purchase_timestamp").isNull())
    .withColumn("_is_status_invalid", F.col("order_status").isNull() | ~F.col("order_status").isin(VALID_STATUS))
    .withColumn("_is_updated_at_invalid", try_ts("source_updated_at").isNull()))
flags = [c for c in flagged.columns if c.startswith("_is_")]

# melt: wide flags -> long (reason, status) -> keep TRUE -> collect the reasons into an array
reasons = (
    flagged.filter(reduce(lambda a, b: a | b, [F.col(f) for f in flags]))
    .dropDuplicates(["_sk"])
    .melt(ids=["_sk"], values=flags, variableColumnName="reason", valueColumnName="status")
    .filter(F.col("status") == True)
    .withColumn("reason", F.regexp_replace("reason", "^_is_", ""))
    .groupBy("_sk")
    .agg(F.collect_list("reason").alias("reason"))
)

bad = flagged.join(reasons, "_sk", "inner").drop(*flags)                          # keeps `reason`
good = flagged.join(reasons.select("_sk"), "_sk", "left_anti").drop(*flags, "_sk")

# Recomputed from all of Bronze every run, so overwrite keeps it idempotent
(bad.withColumn("_quarantined_at", F.current_timestamp())
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("quarantine_orders"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## A) Orders: dedup (exact duplicates and CDC versions) and MERGE

# COMMAND ----------

typed = good.select(
    "order_id", "customer_id", "order_status",
    *[try_ts(c).alias(c) for c in TS_COLS],
    try_ts("source_updated_at").alias("source_updated_at"),
    "_load_dttm")

# One step handles both exact duplicates and several versions of the same order (CDC):
# keep the newest by source_updated_at (tie-break: latest ingestion).
w = Window.partitionBy("order_id").orderBy(F.col("source_updated_at").desc(), F.col("_load_dttm").desc())
latest = (typed.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn", "_load_dttm"))

if not spark.catalog.tableExists("silver_orders"):
    latest.write.format("delta").clusterBy("order_id").saveAsTable("silver_orders")   # Liquid Clustering: MERGE key
else:
    (DeltaTable.forName(spark, "silver_orders").alias("t")
     .merge(latest.alias("s"), "t.order_id = s.order_id")
     .whenMatchedUpdateAll(condition="s.source_updated_at > t.source_updated_at")   # only if newer
     .whenNotMatchedInsertAll()
     .execute())

# COMMAND ----------

# MAGIC %md
# MAGIC ## B) Customers: SCD Type 2 (dim_customer)

# COMMAND ----------

ADDR = ["customer_zip_code_prefix", "customer_city", "customer_state"]

# latest version of each customer_id from Bronze (batch 2 rows win over batch 1)
w = Window.partitionBy("customer_id").orderBy(F.col("source_updated_at").desc(), F.col("_load_dttm").desc())
src = (spark.table("bronze_customers")
       .withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
       .select("customer_id", "customer_unique_id", *ADDR))

if not spark.catalog.tableExists("dim_customer"):
    (src.withColumn("valid_from", F.current_timestamp())
        .withColumn("valid_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .write.format("delta").clusterBy("customer_id").saveAsTable("dim_customer"))   # Liquid Clustering: MERGE key
else:
    changed_sql = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in ADDR)       # <=> is NULL-safe
    current = spark.table("dim_customer").filter("is_current")
    changed = (src.alias("s").join(current.alias("t"), "customer_id")
               .where(changed_sql).select("s.*"))

    # Staged merge: changed rows with a NULL key never match -> INSERT the new version;
    # all rows with their real key match the current row -> CLOSE the old version.
    staged = (changed.withColumn("merge_key", F.lit(None).cast("string"))
              .unionByName(src.withColumn("merge_key", F.col("customer_id"))))

    (DeltaTable.forName(spark, "dim_customer").alias("t")
     .merge(staged.alias("s"), "t.customer_id = s.merge_key AND t.is_current = true")
     .whenMatchedUpdate(condition=changed_sql,
                        set={"is_current": F.lit(False), "valid_to": F.current_timestamp()})
     .whenNotMatchedInsert(values={
         "customer_id": "s.customer_id",
         "customer_unique_id": "s.customer_unique_id",
         "customer_zip_code_prefix": "s.customer_zip_code_prefix",
         "customer_city": "s.customer_city",
         "customer_state": "s.customer_state",
         "valid_from": F.current_timestamp(),
         "valid_to": F.lit(None).cast("timestamp"),
         "is_current": F.lit(True)})
     .execute())

# COMMAND ----------

for t in ["silver_orders", "quarantine_orders", "dim_customer"]:
    print(t, spark.table(t).count())