-- TPC-DS Query 48
-- Compute aggregate store sales for transactions matching specific demographic
-- and address criteria.
SELECT SUM(ss_quantity) AS total_quantity
FROM store_sales
JOIN store ON ss_store_sk = s_store_sk
JOIN date_dim ON ss_sold_date_sk = d_date_sk
JOIN customer_demographics ON ss_cdemo_sk = cd_demo_sk
JOIN customer_address ON ss_addr_sk = ca_address_sk
WHERE d_year = 2000
  AND s_state IN ('TN', 'KY', 'GA', 'AL', 'SD')
  AND (
    (cd_marital_status = 'M' AND cd_education_status = '4 yr Degree'
      AND ss_sales_price BETWEEN 100.00 AND 150.00)
    OR (cd_marital_status = 'D' AND cd_education_status = 'Primary'
      AND ss_sales_price BETWEEN 50.00 AND 100.00)
    OR (cd_marital_status = 'S' AND cd_education_status = 'Advanced Degree'
      AND ss_sales_price BETWEEN 150.00 AND 200.00)
  )
  AND (
    (ca_country = 'United States' AND ca_state IN ('KY', 'GA', 'NM')
      AND ss_net_profit BETWEEN 0 AND 2000)
    OR (ca_country = 'United States' AND ca_state IN ('MT', 'OR', 'IN')
      AND ss_net_profit BETWEEN 150 AND 3000)
    OR (ca_country = 'United States' AND ca_state IN ('WI', 'MO', 'WV')
      AND ss_net_profit BETWEEN 50 AND 25000)
  )
LIMIT 100