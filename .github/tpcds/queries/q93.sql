-- TPC-DS Query 93
-- Store sales with reason-based returns
SELECT
  ss_customer_sk,
  SUM(act_sales) AS total_sales
FROM (
  SELECT
    ss_item_sk,
    ss_ticket_number,
    ss_customer_sk,
    CASE WHEN sr_return_quantity IS NOT NULL
         THEN (ss_quantity - sr_return_quantity) * ss_sales_price
         ELSE ss_quantity * ss_sales_price
    END AS act_sales
  FROM store_sales
  LEFT JOIN store_returns
    ON sr_item_sk = ss_item_sk AND sr_ticket_number = ss_ticket_number
  LEFT JOIN reason ON sr_reason_sk = r_reason_sk
  WHERE r_reason_desc = 'reason 28'
) t
GROUP BY ss_customer_sk
ORDER BY total_sales, ss_customer_sk
LIMIT 100
