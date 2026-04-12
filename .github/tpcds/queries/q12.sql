-- TPC-DS Query 12
-- Compute the revenue ratios across item categories for web sales in a given year.
SELECT
  i_item_id,
  i_item_desc,
  i_category,
  i_class,
  i_current_price,
  SUM(ws_ext_sales_price) AS itemrevenue,
  SUM(ws_ext_sales_price) * 100.0 / SUM(SUM(ws_ext_sales_price)) OVER (PARTITION BY i_class) AS revenueratio
FROM web_sales
JOIN item ON ws_item_sk = i_item_sk
JOIN date_dim ON ws_sold_date_sk = d_date_sk
WHERE i_category IN ('Sports', 'Books', 'Home')
  AND d_date BETWEEN '1999-02-22' AND '1999-03-24'
GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price
ORDER BY i_category, i_class, i_item_id, i_item_desc, revenueratio
LIMIT 100
