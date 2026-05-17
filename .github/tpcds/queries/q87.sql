-- TPC-DS Query 87
-- Count of customers who purchased from store but not web or catalog
SELECT COUNT(*) AS count_val
FROM (
  SELECT DISTINCT c_last_name, c_first_name, d_date
  FROM store_sales
  JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
  JOIN customer ON store_sales.ss_customer_sk = customer.c_customer_sk
  WHERE d_month_seq BETWEEN 1200 AND 1200 + 11

  EXCEPT

  SELECT DISTINCT c_last_name, c_first_name, d_date
  FROM catalog_sales
  JOIN date_dim ON catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
  JOIN customer ON catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
  WHERE d_month_seq BETWEEN 1200 AND 1200 + 11

  EXCEPT

  SELECT DISTINCT c_last_name, c_first_name, d_date
  FROM web_sales
  JOIN date_dim ON web_sales.ws_sold_date_sk = date_dim.d_date_sk
  JOIN customer ON web_sales.ws_bill_customer_sk = customer.c_customer_sk
  WHERE d_month_seq BETWEEN 1200 AND 1200 + 11
) cool_cust
LIMIT 100
