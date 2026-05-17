-- TPC-DS Query 67
-- Compute the top products ranked by store and department using a rollup
SELECT *
FROM (
  SELECT
    i_category,
    i_class,
    i_brand,
    i_product_name,
    d_year,
    d_qoy,
    d_moy,
    s_store_id,
    SUM(COALESCE(ss_sales_price * ss_quantity, 0)) AS sumsales,
    RANK() OVER (
      PARTITION BY i_category
      ORDER BY SUM(COALESCE(ss_sales_price * ss_quantity, 0)) DESC
    ) AS rk
  FROM store_sales
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  JOIN store ON ss_store_sk = s_store_sk
  JOIN item ON ss_item_sk = i_item_sk
  WHERE d_month_seq BETWEEN 1200 AND 1200 + 11
  GROUP BY ROLLUP(
    i_category, i_class, i_brand, i_product_name,
    d_year, d_qoy, d_moy, s_store_id
  )
) dw1
WHERE rk <= 100
ORDER BY
  i_category,
  i_class,
  i_brand,
  i_product_name,
  d_year,
  d_qoy,
  d_moy,
  s_store_id,
  sumsales,
  rk
LIMIT 100
