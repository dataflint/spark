-- TPC-DS Query 71
-- Brand sales by time of day across channels
SELECT
  i_brand_id AS brand_id,
  i_brand AS brand,
  t_hour,
  t_minute,
  SUM(ext_price) AS ext_price
FROM item
JOIN (
  SELECT
    ws_ext_sales_price AS ext_price,
    ws_sold_date_sk AS sold_date_sk,
    ws_item_sk AS sold_item_sk,
    ws_sold_time_sk AS time_sk
  FROM web_sales
  JOIN date_dim ON ws_sold_date_sk = d_date_sk
  WHERE d_moy = 11 AND d_year = 1999

  UNION ALL

  SELECT
    cs_ext_sales_price AS ext_price,
    cs_sold_date_sk AS sold_date_sk,
    cs_item_sk AS sold_item_sk,
    cs_sold_time_sk AS time_sk
  FROM catalog_sales
  JOIN date_dim ON cs_sold_date_sk = d_date_sk
  WHERE d_moy = 11 AND d_year = 1999

  UNION ALL

  SELECT
    ss_ext_sales_price AS ext_price,
    ss_sold_date_sk AS sold_date_sk,
    ss_item_sk AS sold_item_sk,
    ss_sold_time_sk AS time_sk
  FROM store_sales
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  WHERE d_moy = 11 AND d_year = 1999
) tmp ON sold_item_sk = i_item_sk
JOIN time_dim ON time_sk = t_time_sk
WHERE i_manager_id = 1
GROUP BY i_brand, i_brand_id, t_hour, t_minute
ORDER BY ext_price DESC, i_brand_id
LIMIT 100
