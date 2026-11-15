# Retail Lakehouse Practice Project

A beginner-friendly Databricks project that ingests sample retail CSV files, applies basic data quality checks, creates Delta tables, and analyzes sales.

## Project contents

- `data/`: sample customers, products, and orders CSVs.
- `notebooks/module5_retail_lakehouse.py`: Databricks source notebook. Update the input paths and catalog/schema settings before running.
- `sql/warehouse_sales_analysis.sql`: analysis queries to run after the notebook creates the tables.

## Before you start

You need access to a Databricks workspace and compute that can read the uploaded CSVs and write tables. Upload the three CSV files into a Unity Catalog volume (recommended) or another storage location available to your compute. Do not commit credentials or access tokens to this repository.

## Steps

1. Upload `data/customers.csv`, `data/products.csv`, and `data/orders.csv` to a location accessible from your Databricks compute.
2. Open `notebooks/module5_retail_lakehouse.py` in Databricks (or import it from your Git folder).
3. Edit the three `*_PATH` values near the top to the uploaded file paths. Example volume path: `/Volumes/main/default/retail_practice/customers.csv`.
4. Set `CATALOG` and `SCHEMA` to a catalog and schema where you have permission to create tables.
5. Attach compute and run all notebook cells in order. The notebook creates `customers_bronze`, `products_bronze`, `orders_bronze`, cleaned `customers`, `products`, `orders`, and `sales_by_category` Delta tables.
6. Run `sql/warehouse_sales_analysis.sql` in a SQL editor or SQL notebook. Replace `main.default` in the table names if you used another catalog/schema.

## Learning exercises

- Add rows with missing customer or product IDs and observe the quality checks.
- Add a duplicate order ID and inspect the deduplication step.
- Add a new product category and rerun the category analysis.
- Extend the SQL to report revenue by month or city.

## Expected result

The category query should return one row per product category. Revenue is calculated as quantity multiplied by unit price. The CSVs are deliberately small so the project is easy to inspect and rerun.
# databricks-retail-lakehouse-practice
