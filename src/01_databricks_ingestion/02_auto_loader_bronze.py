from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType
)

BASE_PATH = "/Volumes/dev_catalog/ingestion_lab/raw_files"

SOURCE_PATH = f"{BASE_PATH}/incoming"

SCHEMA_PATH = f"{BASE_PATH}/auto_loader_schema"

CHECKPOINT_PATH = f"{BASE_PATH}/auto_loader_checkpoint"

TARGET_TABLE = "dev_catalog.ingestion_lab.bronze_orders_lab"

# Define the source schema
orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("product", StringType(), True),
    StructField("amount", DoubleType(), True)
])

# Read new CSV files using Auto Loader
orders_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", SCHEMA_PATH)
    .schema(orders_schema)
    .load(SOURCE_PATH)
)

# Write ingested records to the Bronze Delta table
query = (
    orders_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
)

query.awaitTermination()

print("Auto Loader ingestion completed!")