-- TPC-DS Query 31
-- Compare store and web sales per county between two years
-- and find counties where web sales grew faster than store sales.
WITH ss AS (
  SELECT
    ca_county,
    d_qoy,
    d_year,
    SUM(ss_ext_sales_price) AS store_sales
  FROM store_sales
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  JOIN customer_address ON ss_addr_sk = ca_address_sk
  GROUP BY ca_county, d_qoy, d_year
),
ws AS (
  SELECT
    ca_county,
    d_qoy,
    d_year,
    SUM(ws_ext_sales_price) AS web_sales
  FROM web_sales
  JOIN date_dim ON ws_sold_date_sk = d_date_sk
  JOIN customer_address ON ws_bill_addr_sk = ca_address_sk
  GROUP BY ca_county, d_qoy, d_year
)
SELECT
  ss1.ca_county,
  ss1.d_year,
  ws2.web_sales / NULLIF(ws1.web_sales, 0) AS web_q1_q2_increase,
  ss2.store_sales / NULLIF(ss1.store_sales, 0) AS store_q1_q2_increase,
  ws3.web_sales / NULLIF(ws1.web_sales, 0) AS web_q1_q3_increase,
  ss3.store_sales / NULLIF(ss1.store_sales, 0) AS store_q1_q3_increase
FROM ss ss1
JOIN ss ss2 ON ss1.ca_county = ss2.ca_county AND ss2.d_qoy = 2 AND ss2.d_year = 2000
JOIN ss ss3 ON ss1.ca_county = ss3.ca_county AND ss3.d_qoy = 3 AND ss3.d_year = 2000
JOIN ws ws1 ON ss1.ca_county = ws1.ca_county AND ws1.d_qoy = 1 AND ws1.d_year = 2000
JOIN ws ws2 ON ss1.ca_county = ws2.ca_county AND ws2.d_qoy = 2 AND ws2.d_year = 2000
JOIN ws ws3 ON ss1.ca_county = ws3.ca_county AND ws3.d_qoy = 3 AND ws3.d_year = 2000
WHERE ss1.d_qoy = 1
  AND ss1.d_year = 2000
  AND CASE WHEN ws1.web_sales > 0
    THEN ws2.web_sales / ws1.web_sales ELSE 0 END
    > CASE WHEN ss1.store_sales > 0
    THEN ss2.store_sales / ss1.store_sales ELSE 0 END
  AND CASE WHEN ws1.web_sales > 0
    THEN ws3.web_sales / ws1.web_sales ELSE 0 END
    > CASE WHEN ss1.store_sales > 0
    THEN ss3.store_sales / ss1.store_sales ELSE 0 END
ORDER BY ss1.ca_county
LIMIT 100
