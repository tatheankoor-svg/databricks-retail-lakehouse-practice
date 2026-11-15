-- Run after module5_retail_lakehouse.py creates the tables.
-- Change workspace.default if your notebook used a different catalog/schema.

-- Revenue and order count by product category.
SELECT
  p.category,
  CAST(SUM(o.quantity * p.unit_price) AS DECIMAL(12, 2)) AS revenue,
  COUNT(DISTINCT o.order_id) AS order_count
FROM workspace.default.orders AS o
JOIN workspace.default.products AS p USING (product_id)
GROUP BY p.category
ORDER BY revenue DESC;

-- Monthly revenue.
SELECT
  DATE_TRUNC('MONTH', o.order_date) AS order_month,
  CAST(SUM(o.quantity * p.unit_price) AS DECIMAL(12, 2)) AS revenue
FROM workspace.default.orders AS o
JOIN workspace.default.products AS p USING (product_id)
GROUP BY DATE_TRUNC('MONTH', o.order_date)
ORDER BY order_month;

-- Revenue by customer city.
SELECT
  c.city,
  CAST(SUM(o.quantity * p.unit_price) AS DECIMAL(12, 2)) AS revenue,
  COUNT(DISTINCT o.order_id) AS order_count
FROM workspace.default.orders AS o
JOIN workspace.default.products AS p USING (product_id)
JOIN workspace.default.customers AS c USING (customer_id)
GROUP BY c.city
ORDER BY revenue DESC;

