-- TPC-DS Query 70
-- Rollup of store sales by state with ranking
SELECT
  SUM(ss_net_profit) AS total_sum,
  s_state,
  s_county,
  GROUPING(s_state) + GROUPING(s_county) AS lochierarchy,
  RANK() OVER (
    PARTITION BY GROUPING(s_state) + GROUPING(s_county),
    CASE WHEN GROUPING(s_county) = 0 THEN s_state END
    ORDER BY SUM(ss_net_profit) DESC
  ) AS rank_within_parent
FROM store_sales
JOIN date_dim d1 ON store_sales.ss_sold_date_sk = d1.d_date_sk
JOIN store ON store_sales.ss_store_sk = store.s_store_sk
WHERE d1.d_month_seq BETWEEN 1200 AND 1200 + 11
  AND s_state IN (
    SELECT s_state
    FROM (
      SELECT
        s_state AS s_state,
        RANK() OVER (PARTITION BY s_state ORDER BY SUM(ss_net_profit) DESC) AS ranking
      FROM store_sales
      JOIN store ON store_sales.ss_store_sk = store.s_store_sk
      JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
      WHERE d_month_seq BETWEEN 1200 AND 1200 + 11
      GROUP BY s_state, s_county
    ) tmp1
    WHERE ranking <= 5
  )
GROUP BY ROLLUP(s_state, s_county)
ORDER BY
  lochierarchy DESC,
  CASE WHEN lochierarchy = 0 THEN s_state END,
  rank_within_parent
LIMIT 100
