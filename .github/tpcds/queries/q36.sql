-- TPC-DS Query 36
-- Compute the gross profit margin ranking of stores by item category for a given year.
SELECT
  SUM(ss_net_profit) / SUM(ss_ext_sales_price) AS gross_margin,
  i_category,
  i_class,
  GROUPING(i_category) + GROUPING(i_class) AS lochierarchy,
  RANK() OVER (
    PARTITION BY GROUPING(i_category) + GROUPING(i_class),
    CASE WHEN GROUPING(i_class) = 0 THEN i_category END
    ORDER BY SUM(ss_net_profit) / SUM(ss_ext_sales_price) ASC
  ) AS rank_within_parent
FROM store_sales
JOIN date_dim d1 ON d1.d_date_sk = ss_sold_date_sk
JOIN item ON ss_item_sk = i_item_sk
JOIN store ON ss_store_sk = s_store_sk
WHERE d1.d_year = 2001
  AND s_state IN ('TN', 'TN', 'TN', 'TN', 'TN', 'TN', 'TN', 'TN')
GROUP BY ROLLUP(i_category, i_class)
ORDER BY
  lochierarchy DESC,
  CASE WHEN lochierarchy = 0 THEN i_category END,
  rank_within_parent
LIMIT 100
