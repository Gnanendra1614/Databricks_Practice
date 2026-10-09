
from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = "/Volumes/dev_catalog/ingestion_lab/raw_files/raw_landing/"


# 1. BRONZE LAYER - Ingest raw order files
@dp.table(comment="Bronze layer - raw orders")
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


# 2. SILVER LAYER - Clean and validate orders
@dp.table(comment="Silver layer - cleaned orders")
def silver_orders():
    return (
        spark.readStream
        .table("bronze_orders")
        .withColumn("order_id", F.col("order_id").cast("int"))
        .withColumn("amount", F.col("amount").cast("double"))
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("product").isNotNull())
        .filter(F.col("amount").isNotNull())
    )


# 3. GOLD LAYER - Aggregate sales by product
@dp.materialized_view(comment="Gold layer - product sales summary")
def gold_product_sales():
    return (
        spark.read.table("silver_orders")
        .groupBy("product")
        .agg(
            F.sum("amount").alias("total_sales"),
            F.count("*").alias("order_count")
        )
    )
