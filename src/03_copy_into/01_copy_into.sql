CREATE TABLE IF NOT EXISTS
    dev_catalog.ingestion_lab.bronze_orders_copy_lab (
        order_id INT,
        customer_name STRING,
        product STRING,
        amount INT
    )
USING DELTA;

-- Load the first batch
COPY INTO dev_catalog.ingestion_lab.bronze_orders_copy_lab
FROM '/Volumes/dev_catalog/ingestion_lab/raw_files/orders_batch_1'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true');

-- Verify the first batch
SELECT *
FROM dev_catalog.ingestion_lab.bronze_orders_copy_lab;

-- Verify the total record count
SELECT COUNT(*) AS total_orders
FROM dev_catalog.ingestion_lab.bronze_orders_copy_lab;