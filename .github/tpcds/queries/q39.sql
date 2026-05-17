-- TPC-DS Query 39
-- Compute the coefficient of variation of inventory for items by warehouse
-- for two consecutive months.
WITH inv AS (
  SELECT
    w_warehouse_name,
    w_warehouse_sk,
    i_item_sk,
    d_moy,
    STDDEV_SAMP(inv_quantity_on_hand) AS stdev,
    AVG(inv_quantity_on_hand) AS mean
  FROM inventory
  JOIN item ON inv_item_sk = i_item_sk
  JOIN warehouse ON inv_warehouse_sk = w_warehouse_sk
  JOIN date_dim ON inv_date_sk = d_date_sk
  WHERE d_year = 2001
  GROUP BY w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy
)
SELECT
  inv1.w_warehouse_sk,
  inv1.i_item_sk,
  inv1.d_moy,
  inv1.mean,
  inv1.stdev,
  inv1.mean / CASE WHEN inv1.stdev IS NOT NULL AND inv1.stdev > 0 THEN inv1.stdev ELSE 1 END AS cov
FROM inv inv1
JOIN inv inv2
  ON inv1.i_item_sk = inv2.i_item_sk
  AND inv1.w_warehouse_sk = inv2.w_warehouse_sk
WHERE inv1.d_moy = 1
  AND inv2.d_moy = 1 + 1
  AND inv1.mean > 0
  AND CASE WHEN inv1.mean > 0 THEN inv1.stdev / inv1.mean ELSE NULL END > 1
ORDER BY inv1.w_warehouse_sk, inv1.i_item_sk, inv1.d_moy
LIMIT 100
