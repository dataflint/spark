-- TPC-DS Query 44
-- Find pairs of items that are frequently sold together in stores and have
-- the best and worst net profit ranking.
SELECT
  asceding.rnk,
  i1.i_product_name AS best,
  i2.i_product_name AS worst
FROM (
  SELECT *,
    RANK() OVER (ORDER BY ss_net_profit_rank ASC) AS rnk
  FROM (
    SELECT
      ss_item_sk,
      AVG(ss_net_profit) AS ss_net_profit_rank
    FROM store_sales
    WHERE ss_store_sk = 4
    GROUP BY ss_item_sk
    HAVING AVG(ss_net_profit) > 0.9 * (
      SELECT AVG(ss_net_profit) FROM store_sales WHERE ss_store_sk = 4 AND ss_addr_sk IS NULL
    )
  ) tmp1
) asceding
JOIN (
  SELECT *,
    RANK() OVER (ORDER BY ss_net_profit_rank DESC) AS rnk
  FROM (
    SELECT
      ss_item_sk,
      AVG(ss_net_profit) AS ss_net_profit_rank
    FROM store_sales
    WHERE ss_store_sk = 4
    GROUP BY ss_item_sk
    HAVING AVG(ss_net_profit) > 0.9 * (
      SELECT AVG(ss_net_profit) FROM store_sales WHERE ss_store_sk = 4 AND ss_addr_sk IS NULL
    )
  ) tmp2
) descending ON asceding.rnk = descending.rnk
JOIN item i1 ON asceding.ss_item_sk = i1.i_item_sk
JOIN item i2 ON descending.ss_item_sk = i2.i_item_sk
ORDER BY asceding.rnk
LIMIT 100
