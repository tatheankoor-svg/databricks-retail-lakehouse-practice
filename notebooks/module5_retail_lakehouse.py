# Databricks notebook source
# Retail Lakehouse Practice: CSV -> Bronze Delta -> cleaned Silver Delta -> sales summary
# Before running, set the three file paths and catalog/schema below.

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Update these paths to match where you uploaded the CSVs.
CUSTOMERS_PATH = "/Volumes/main/default/retail_practice/customers.csv"
PRODUCTS_PATH = "/Volumes/main/default/retail_practice/products.csv"
ORDERS_PATH = "/Volumes/main/default/retail_practice/orders.csv"

# Update these to a catalog and schema where you can create tables.
CATALOG = "main"
SCHEMA = "default"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------
def read_csv(path):
    return (spark.read
            .option("header", True)
            .option("inferSchema", True)
            .option("mode", "PERMISSIVE")
            .csv(path))

customers_raw = read_csv(CUSTOMERS_PATH)
products_raw = read_csv(PRODUCTS_PATH)
orders_raw = read_csv(ORDERS_PATH)

# Keep raw ingested data in Bronze tables for traceability.
(customers_raw.write.format("delta").mode("overwrite")
 .saveAsTable(f"{CATALOG}.{SCHEMA}.customers_bronze"))
(products_raw.write.format("delta").mode("overwrite")
 .saveAsTable(f"{CATALOG}.{SCHEMA}.products_bronze"))
(orders_raw.write.format("delta").mode("overwrite")
 .saveAsTable(f"{CATALOG}.{SCHEMA}.orders_bronze"))

# COMMAND ----------
# Clean types, remove incomplete records, and keep one row per business key.
customers = (customers_raw
    .select(F.col("customer_id").cast("long").alias("customer_id"),
            F.trim("name").alias("name"), F.lower(F.trim("email")).alias("email"),
            F.initcap(F.trim("city")).alias("city"))
    .filter(F.col("customer_id").isNotNull() & F.col("name").isNotNull())
    .dropDuplicates(["customer_id"]))

products = (products_raw
    .select(F.col("product_id").cast("long").alias("product_id"),
            F.trim("product_name").alias("product_name"),
            F.trim("category").alias("category"),
            F.col("unit_price").cast("decimal(10,2)").alias("unit_price"))
    .filter(F.col("product_id").isNotNull() & F.col("unit_price").isNotNull()
            & (F.col("unit_price") > 0))
    .dropDuplicates(["product_id"]))

orders_typed = (orders_raw
    .select(F.col("order_id").cast("long").alias("order_id"),
            F.col("customer_id").cast("long").alias("customer_id"),
            F.col("product_id").cast("long").alias("product_id"),
            F.col("quantity").cast("int").alias("quantity"),
            F.to_date("order_date").alias("order_date")))

orders = (orders_typed
    .filter(F.col("order_id").isNotNull() & F.col("customer_id").isNotNull()
            & F.col("product_id").isNotNull() & F.col("quantity").isNotNull()
            & (F.col("quantity") > 0) & F.col("order_date").isNotNull())
    .dropDuplicates(["order_id"]))

# Retain only orders whose customer and product keys exist.
orders = (orders.join(customers.select("customer_id"), "customer_id", "inner")
                .join(products.select("product_id"), "product_id", "inner"))

# COMMAND ----------
for name, df in [("customers", customers), ("products", products), ("orders", orders)]:
    (df.write.format("delta").mode("overwrite")
       .saveAsTable(f"{CATALOG}.{SCHEMA}.{name}"))

# Build a reusable sales summary table.
sales = (orders.join(products, "product_id")
    .join(customers.select("customer_id", "city"), "customer_id")
    .withColumn("revenue", F.col("quantity") * F.col("unit_price"))
    .groupBy("category")
    .agg(F.sum("revenue").cast("decimal(12,2)").alias("revenue"),
         F.countDistinct("order_id").alias("order_count"))
    .orderBy(F.desc("revenue")))

(sales.write.format("delta").mode("overwrite")
 .saveAsTable(f"{CATALOG}.{SCHEMA}.sales_by_category"))

# COMMAND ----------
# Inspect outputs and row counts.
display(sales)

for table in ["customers", "products", "orders", "sales_by_category"]:
    print(table, spark.table(f"{CATALOG}.{SCHEMA}.{table}").count())
