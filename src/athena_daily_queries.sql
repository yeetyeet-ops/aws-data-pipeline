-- name: sanity_sample_rows
-- 1) Quick sanity check: sample rows
SELECT *
FROM shopmart_sales.good_data
LIMIT 20;

-- name: single_partition_rows
-- 2) Rows in one partition (fast + cheap due to partition pruning)
SELECT *
FROM shopmart_sales.good_data
WHERE store = '01'
  AND "date" = DATE_FORMAT(current_date, '%Y-%m-%d')
LIMIT 50;

-- name: store_daily_revenue
-- 3) Revenue by day for one store
SELECT
  "date",
  SUM(quantity * unit_price) AS revenue
FROM shopmart_sales.good_data
WHERE store = '01'
GROUP BY "date"
ORDER BY "date";

-- name: day_store_revenue
-- 4) Revenue by store for one day
SELECT
  store,
  SUM(quantity * unit_price) AS revenue
FROM shopmart_sales.good_data
WHERE "date" = DATE_FORMAT(current_date, '%Y-%m-%d')
GROUP BY store
ORDER BY revenue DESC;

-- name: top_products_month
-- 5) Top 10 products by revenue (partition-filtered)
SELECT
  product_id,
  SUM(quantity * unit_price) AS total_revenue
FROM shopmart_sales.good_data
WHERE "date" BETWEEN DATE_FORMAT(date_trunc('month', current_date), '%Y-%m-%d')
  AND DATE_FORMAT(last_day_of_month(current_date), '%Y-%m-%d')
GROUP BY product_id
ORDER BY total_revenue DESC
LIMIT 10;

-- name: rolling_7d_metadata_summary
-- 6) Rolling 7-day processing summary from Glue catalog table metadata
SELECT
  "date",
  store,
  COUNT(*) AS files_processed,
  SUM(total_rows) AS total_rows,
  SUM(good_rows) AS good_rows,
  SUM(bad_rows) AS bad_rows,
  ROUND(AVG(bad_row_rate), 4) AS avg_bad_row_rate,
  ROUND(AVG(payment_success_rate), 4) AS avg_payment_success_rate,
  ROUND(AVG(duration_ms), 2) AS avg_duration_ms
FROM shopmart_sales.metadata
WHERE "date" >= DATE_FORMAT(current_date - INTERVAL '7' DAY, '%Y-%m-%d')
GROUP BY "date", store
ORDER BY "date" DESC, store;
