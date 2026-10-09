volume_path = "/Volumes/dev_catalog/ingestion_lab/raw_files"
incoming_path = f"{volume_path}/incoming"
second_batch_path = f"{incoming_path}/orders_batch_2"

# Create three new orders
new_orders = [
    (106, "Rahul", "Mouse", 800.0),
    (107, "Divya", "Tablet", 18000.0),
    (108, "Manoj", "Printer", 9500.0)
]

columns = [
    "order_id",
    "customer_name",
    "product",
    "amount"
]

new_orders_df = spark.createDataFrame(new_orders, columns)

# Display the new records
display(new_orders_df)

# Ensure the incoming folder exists
dbutils.fs.mkdirs(incoming_path)

# Save the new records as CSV files
new_orders_df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(second_batch_path)

print("Second batch created successfully!")
print(f"Source folder: {second_batch_path}")
