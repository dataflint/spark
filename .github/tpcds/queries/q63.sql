-- TPC-DS Query 63
-- Compute the average quarterly revenue for items within certain manufacturer IDs
-- and report those with revenue deviation from the average (similar to q53 with different params).
SELECT *
FROM (
  SELECT
    i_manager_id,
    SUM(ss_sales_price) AS sum_sales,
    AVG(SUM(ss_sales_price)) OVER (PARTITION BY i_manager_id) AS avg_monthly_sales
  FROM item
  JOIN store_sales ON ss_item_sk = i_item_sk
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  JOIN store ON ss_store_sk = s_store_sk
  WHERE d_month_seq IN (1200, 1201, 1202, 1203, 1204, 1205, 1206, 1207, 1208, 1209, 1210, 1211)
    AND (
      (i_category IN ('Books', 'Children', 'Electronics')
        AND i_class IN ('personal', 'portable', 'reference', 'self-help')
        AND i_brand IN ('scholaramalgamalg #14', 'scholaramalgamalg #7',
                        'exportiunivamalg #9', 'scholaramalgamalg #9'))
      OR
      (i_category IN ('Women', 'Music', 'Men')
        AND i_class IN ('accessories', 'classical', 'fragrances', 'pants')
        AND i_brand IN ('amalgimporto #1', 'edu packscholar #1',
                        'exportiimporto #1', 'importoamalg #1'))
    )
  GROUP BY i_manager_id, d_moy
) tmp1
WHERE CASE WHEN avg_monthly_sales > 0 THEN ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales ELSE NULL END > 0.1
ORDER BY i_manager_id, avg_monthly_sales, sum_sales
LIMIT 100
