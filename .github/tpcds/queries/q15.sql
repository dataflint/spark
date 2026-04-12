-- TPC-DS Query 15
-- Report the total catalog sales for customers in selected geographic regions.
SELECT
  ca_zip,
  SUM(cs_sales_price) AS total_sales
FROM catalog_sales
JOIN customer ON cs_bill_customer_sk = c_customer_sk
JOIN customer_address ON c_current_addr_sk = ca_address_sk
JOIN date_dim ON cs_sold_date_sk = d_date_sk
WHERE (SUBSTR(ca_zip, 1, 5) IN ('85669', '86197', '88274', '83405', '86475',
                                  '85392', '85460', '80348', '81792')
       OR ca_state IN ('CA', 'WA', 'GA')
       OR cs_sales_price > 500)
  AND d_qoy = 2
  AND d_year = 2001
GROUP BY ca_zip
ORDER BY ca_zip
LIMIT 100
