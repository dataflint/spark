-- TPC-DS Query 3
-- Report the total extended sales price per item brand of a specific manufacturer
-- for all sales in a specific month of the year.
SELECT
  d_year,
  i_brand_id AS brand_id,
  i_brand AS brand,
  SUM(ss_ext_sales_price) AS sum_agg
FROM date_dim
JOIN store_sales ON d_date_sk = ss_sold_date_sk
JOIN item ON ss_item_sk = i_item_sk
WHERE i_manufact_id = 128
  AND d_moy = 11
GROUP BY d_year, i_brand_id, i_brand
ORDER BY d_year, sum_agg DESC, brand_id
LIMIT 100
