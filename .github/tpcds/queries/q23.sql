-- TPC-DS Query 23 (simplified)
-- Find customers whose total spending is in the top 5% and who have
-- purchased items from a frequently sold subset, across store and web channels.
WITH frequent_ss_items AS (
  SELECT
    i_item_sk AS substr_item_sk
  FROM item
  JOIN store_sales ON i_item_sk = ss_item_sk
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  WHERE d_year IN (2000, 2001, 2002, 2003)
  GROUP BY i_item_sk
  HAVING COUNT(*) > 4
),
max_store_sales AS (
  SELECT MAX(csales) AS tpcds_cmax
  FROM (
    SELECT
      c_customer_sk,
      SUM(ss_quantity * ss_sales_price) AS csales
    FROM customer
    JOIN store_sales ON c_customer_sk = ss_customer_sk
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    WHERE d_year IN (2000, 2001, 2002, 2003)
    GROUP BY c_customer_sk
  ) x
),
best_ss_customer AS (
  SELECT c_customer_sk AS ssales_customer_sk
  FROM customer
  JOIN store_sales ON c_customer_sk = ss_customer_sk
  GROUP BY c_customer_sk
  HAVING SUM(ss_quantity * ss_sales_price) > 0.95 * (SELECT tpcds_cmax FROM max_store_sales)
)
SELECT SUM(sales) AS total_sales
FROM (
  SELECT cs_quantity * cs_list_price AS sales
  FROM catalog_sales
  JOIN date_dim ON cs_sold_date_sk = d_date_sk
  WHERE d_year = 2000 AND d_moy = 2
    AND cs_item_sk IN (SELECT substr_item_sk FROM frequent_ss_items)
    AND cs_bill_customer_sk IN (SELECT ssales_customer_sk FROM best_ss_customer)
  UNION ALL
  SELECT ws_quantity * ws_list_price AS sales
  FROM web_sales
  JOIN date_dim ON ws_sold_date_sk = d_date_sk
  WHERE d_year = 2000 AND d_moy = 2
    AND ws_item_sk IN (SELECT substr_item_sk FROM frequent_ss_items)
    AND ws_bill_customer_sk IN (SELECT ssales_customer_sk FROM best_ss_customer)
) y
LIMIT 100
