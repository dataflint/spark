-- TPC-DS Query 21
-- For each warehouse and item, report the inventory levels
-- exceeding expected values for a given date range.
SELECT *
FROM (
  SELECT
    w_warehouse_name,
    i_item_id,
    SUM(CASE WHEN d_date < '2000-03-11'
      THEN inv_quantity_on_hand ELSE 0 END) AS inv_before,
    SUM(CASE WHEN d_date >= '2000-03-11'
      THEN inv_quantity_on_hand ELSE 0 END) AS inv_after
  FROM inventory
  JOIN warehouse ON inv_warehouse_sk = w_warehouse_sk
  JOIN item ON inv_item_sk = i_item_sk
  JOIN date_dim ON inv_date_sk = d_date_sk
  WHERE d_date BETWEEN '2000-02-10' AND '2000-04-10'
  GROUP BY w_warehouse_name, i_item_id
) x
WHERE (CASE WHEN inv_before > 0
  THEN inv_after / inv_before ELSE NULL END) BETWEEN 2.0/3.0 AND 3.0/2.0
ORDER BY w_warehouse_name, i_item_id
LIMIT 100
