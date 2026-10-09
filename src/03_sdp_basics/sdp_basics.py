
from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = "/Volumes/dev_catalog/ingestion_lab/raw_files/raw_landing/"

# 1. Streaming table: ingest raw CSV orders
@dp.table(comment="Bronze orders from raw CSV files")
def bronze_orders():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option(
            "cloudFiles.schemaLocation",
            "/Volumes/dev_catalog/ingestion_lab/raw_files/bronze_schema"
        )
        .load(LANDING)
    )


# 2. Materialized view: keep valid orders
@dp.materialized_view(comment="Silver orders with valid values")
def silver_orders():
    return (
        spark.read.table("bronze_orders")
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("product").isNotNull())
        .filter(F.col("amount").isNotNull())
        .withColumn("order_id", F.col("order_id").cast("int"))
        .withColumn("amount", F.col("amount").cast("double"))
    )


# 3. Materialized view: summarize sales by product
@dp.materialized_view(comment="Gold product sales summary")
def gold_product_sales():
    return (
        spark.read.table("silver_orders")
        .groupBy("product")
        .agg(
            F.sum("amount").alias("total_sales"),
            F.count("*").alias("order_count")
        )
    )
