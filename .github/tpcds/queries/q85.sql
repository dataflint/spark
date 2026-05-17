-- TPC-DS Query 85
-- Web sales returns analysis by customer demographics and reason
SELECT
  SUBSTR(r_reason_desc, 1, 20) AS reason,
  AVG(ws_quantity) AS avg_qty,
  AVG(wr_refunded_cash) AS avg_refund,
  AVG(wr_fee) AS avg_fee
FROM web_sales
JOIN web_returns ON ws_item_sk = wr_item_sk AND ws_order_number = wr_order_number
JOIN web_page ON ws_web_page_sk = wp_web_page_sk
JOIN customer_demographics cd1 ON ws_bill_cdemo_sk = cd1.cd_demo_sk
JOIN customer_demographics cd2 ON wr_refunded_cdemo_sk = cd2.cd_demo_sk
JOIN customer_address ON wr_refunded_addr_sk = ca_address_sk
JOIN date_dim ON ws_sold_date_sk = d_date_sk
JOIN reason ON wr_reason_sk = r_reason_sk
WHERE d_year = 2000
  AND (
    (cd1.cd_marital_status = 'M' AND cd1.cd_education_status = 'Advanced Degree'
     AND ws_sales_price BETWEEN 100.00 AND 150.00)
    OR
    (cd1.cd_marital_status = 'S' AND cd1.cd_education_status = 'College'
     AND ws_sales_price BETWEEN 50.00 AND 100.00)
    OR
    (cd1.cd_marital_status = 'W' AND cd1.cd_education_status = '2 yr Degree'
     AND ws_sales_price BETWEEN 150.00 AND 200.00)
  )
  AND (
    (ca_country = 'United States' AND ca_state IN ('IN', 'OH', 'NJ')
     AND ws_net_profit BETWEEN 100 AND 200)
    OR
    (ca_country = 'United States' AND ca_state IN ('WI', 'CT', 'KY')
     AND ws_net_profit BETWEEN 150 AND 300)
    OR
    (ca_country = 'United States' AND ca_state IN ('LA', 'IA', 'AR')
     AND ws_net_profit BETWEEN 50 AND 250)
  )
  AND cd1.cd_marital_status = cd2.cd_marital_status
  AND cd1.cd_education_status = cd2.cd_education_status
GROUP BY r_reason_desc
ORDER BY reason, avg_qty, avg_refund, avg_fee
LIMIT 100
