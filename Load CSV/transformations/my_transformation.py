"""Retail CSV-to-Delta ETL script for Databricks.

This script reads the sample CSV files from the linked Databricks Git folder,
creates raw Bronze tables, creates cleaned tables, and builds a category sales
summary. Update PROJECT_ROOT if the Git folder is renamed or moved.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

PROJECT_ROOT = "/Workspace/Users/tatheankoor@gmail.com/databricks-retail-lakehouse-practice"
DATA_ROOT = f"{PROJECT_ROOT}/data"
CATALOG = "workspace"
SCHEMA = "default"


def read_csv(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("mode", "PERMISSIVE")
        .csv(f"file:{path}")
    )


def write_table(df: DataFrame, table_name: str) -> None:
    full_name = f"{CATALOG}.{SCHEMA}.{table_name}"
    df.write.format("delta").mode("overwrite").saveAsTable(full_name)
    print(f"Wrote {full_name}: {df.count()} rows")


def run_etl(spark: SparkSession) -> None:
    # Create the target schema if it does not already exist.
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

    customers_raw = read_csv(spark, f"{DATA_ROOT}/customers.csv")
    products_raw = read_csv(spark, f"{DATA_ROOT}/products.csv")
    orders_raw = read_csv(spark, f"{DATA_ROOT}/orders.csv")

    # Bronze layer: retain the source data as ingested.
    write_table(customers_raw, "customers_bronze")
    write_table(products_raw, "products_bronze")
    write_table(orders_raw, "orders_bronze")

    # Silver layer: clean values, set types, filter invalid rows, deduplicate.
    customers = (
        customers_raw.select(
            F.col("customer_id").cast("long").alias("customer_id"),
            F.trim("name").alias("name"),
            F.lower(F.trim("email")).alias("email"),
            F.initcap(F.trim("city")).alias("city"),
        )
        .filter(F.col("customer_id").isNotNull() & F.col("name").isNotNull())
        .dropDuplicates(["customer_id"])
    )

    products = (
        products_raw.select(
            F.col("product_id").cast("long").alias("product_id"),
            F.trim("product_name").alias("product_name"),
            F.trim("category").alias("category"),
            F.col("unit_price").cast("decimal(10,2)").alias("unit_price"),
        )
        .filter(
            F.col("product_id").isNotNull()
            & F.col("unit_price").isNotNull()
            & (F.col("unit_price") > 0)
        )
        .dropDuplicates(["product_id"])
    )

    orders = (
        orders_raw.select(
            F.col("order_id").cast("long").alias("order_id"),
            F.col("customer_id").cast("long").alias("customer_id"),
            F.col("product_id").cast("long").alias("product_id"),
            F.col("quantity").cast("int").alias("quantity"),
            F.to_date("order_date").alias("order_date"),
        )
        .filter(
            F.col("order_id").isNotNull()
            & F.col("customer_id").isNotNull()
            & F.col("product_id").isNotNull()
            & F.col("quantity").isNotNull()
            & (F.col("quantity") > 0)
            & F.col("order_date").isNotNull()
        )
        .dropDuplicates(["order_id"])
    )

    # Keep orders only when the referenced customer and product exist.
    orders = (
        orders.join(customers.select("customer_id"), "customer_id", "inner")
        .join(products.select("product_id"), "product_id", "inner")
    )

    write_table(customers, "customers")
    write_table(products, "products")
    write_table(orders, "orders")

    # Gold layer: category revenue and distinct order count.
    sales_by_category = (
        orders.join(products, "product_id")
        .withColumn("revenue", F.col("quantity") * F.col("unit_price"))
        .groupBy("category")
        .agg(
            F.sum("revenue").cast("decimal(12,2)").alias("revenue"),
            F.countDistinct("order_id").alias("order_count"),
        )
        .orderBy(F.desc("revenue"))
    )
    write_table(sales_by_category, "sales_by_category")
    sales_by_category.show(truncate=False)


def main() -> None:
    spark = SparkSession.builder.appName("RetailLakehouseETL").getOrCreate()
    run_etl(spark)


if __name__ == "__main__":
    main()
