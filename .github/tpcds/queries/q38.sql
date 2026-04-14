-- TPC-DS Query 38
-- Count distinct customers who purchased items from all three channels (store, catalog, web)
-- in a given year.
SELECT COUNT(*) AS cnt
FROM (
  SELECT DISTINCT c_last_name, c_first_name, d_date
  FROM store_sales
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  JOIN customer ON ss_customer_sk = c_customer_sk
  WHERE d_month_seq BETWEEN 1200 AND 1200 + 11
  INTERSECT
  SELECT DISTINCT c_last_name, c_first_name, d_date
  FROM catalog_sales
  JOIN date_dim ON cs_sold_date_sk = d_date_sk
  JOIN customer ON cs_bill_customer_sk = c_customer_sk
  WHERE d_month_seq BETWEEN 1200 AND 1200 + 11
  INTERSECT
  SELECT DISTINCT c_last_name, c_first_name, d_date
  FROM web_sales
  JOIN date_dim ON ws_sold_date_sk = d_date_sk
  JOIN customer ON ws_bill_customer_sk = c_customer_sk
  WHERE d_month_seq BETWEEN 1200 AND 1200 + 11
) hot_cust
LIMIT 100
