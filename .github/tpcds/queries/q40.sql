-- TPC-DS Query 40
-- Compute catalog sales and returns by item and warehouse for items sold within
-- 30 days of a given date.
SELECT
  w_state,
  i_item_id,
  SUM(CASE WHEN d_date < '2000-03-11'
    THEN cs_sales_price - COALESCE(cr_refunded_cash, 0) ELSE 0 END) AS sales_before,
  SUM(CASE WHEN d_date >= '2000-03-11'
    THEN cs_sales_price - COALESCE(cr_refunded_cash, 0) ELSE 0 END) AS sales_after
FROM catalog_sales
LEFT JOIN catalog_returns ON cs_order_number = cr_order_number AND cs_item_sk = cr_item_sk
JOIN warehouse ON cs_warehouse_sk = w_warehouse_sk
JOIN item ON cs_item_sk = i_item_sk
JOIN date_dim ON cs_sold_date_sk = d_date_sk
WHERE d_date BETWEEN DATE_SUB('2000-03-11', 30) AND DATE_ADD('2000-03-11', 30)
  AND i_current_price BETWEEN 0.99 AND 1.49
GROUP BY w_state, i_item_id
ORDER BY w_state, i_item_id
LIMIT 100
