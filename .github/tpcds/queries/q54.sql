-- TPC-DS Query 54
-- Find the monthly revenue from catalog and web sales by customers who also
-- purchased specific categories in stores during a certain time period.
WITH my_customers AS (
  SELECT DISTINCT c_customer_sk, c_current_addr_sk
  FROM (
    SELECT cs_sold_date_sk AS sold_date_sk, cs_bill_customer_sk AS customer_sk, cs_item_sk AS item_sk
    FROM catalog_sales
    UNION ALL
    SELECT ws_sold_date_sk AS sold_date_sk, ws_bill_customer_sk AS customer_sk, ws_item_sk AS item_sk
    FROM web_sales
  ) cs_or_ws_sales
  JOIN item ON item_sk = i_item_sk
  JOIN date_dim ON sold_date_sk = d_date_sk
  JOIN customer ON customer_sk = c_customer_sk
  WHERE i_category = 'Women'
    AND i_class = 'maternity'
    AND d_moy = 12
    AND d_year = 1998
),
my_revenue AS (
  SELECT
    c_customer_sk,
    SUM(ss_ext_sales_price) AS revenue
  FROM my_customers
  JOIN store_sales ON c_customer_sk = ss_customer_sk
  JOIN customer_address ON c_current_addr_sk = ca_address_sk
  JOIN store ON ca_state = s_state AND s_market_id = 8
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  WHERE d_year = 1998 AND d_moy BETWEEN 4 AND 10
  GROUP BY c_customer_sk
)
SELECT
  COUNT(*) AS customer_count,
  SUM(revenue) AS total_revenue
FROM (
  SELECT
    c_customer_sk,
    revenue,
    NTILE(50) OVER (ORDER BY revenue) AS segment
  FROM my_revenue
) segmented
WHERE segment = 1 -- Could be any segment; using 1 for the lowest-revenue segment
LIMIT 100
