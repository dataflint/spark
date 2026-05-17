-- TPC-DS Query 45
-- Report web sales for customers in specific zip codes where the items were
-- purchased from a specific set of item identifiers, grouped by customer address attributes.
SELECT
  ca_zip,
  ca_city,
  SUM(ws_sales_price) AS total_web_sales
FROM web_sales
JOIN customer ON ws_bill_customer_sk = c_customer_sk
JOIN customer_address ON c_current_addr_sk = ca_address_sk
JOIN date_dim ON ws_sold_date_sk = d_date_sk
JOIN item ON ws_item_sk = i_item_sk
WHERE d_qoy = 2
  AND d_year = 2001
  AND (
    SUBSTR(ca_zip, 1, 5) IN ('85669', '86197', '88274', '83405', '86475',
                              '85392', '85460', '80348', '81792')
    OR i_item_id IN (
      SELECT i_item_id
      FROM item
      WHERE i_item_sk IN (2, 3, 5, 7, 11, 13, 17, 19, 23, 29)
    )
  )
GROUP BY ca_zip, ca_city
ORDER BY ca_zip, ca_city
LIMIT 100
