-- TPC-DS Query 98
-- Store sales by item category, class, and department with window functions
SELECT
  i_item_id,
  i_item_desc,
  i_category,
  i_class,
  i_current_price,
  SUM(ss_ext_sales_price) AS itemrevenue,
  SUM(ss_ext_sales_price) * 100.0 / SUM(SUM(ss_ext_sales_price)) OVER (PARTITION BY i_class) AS revenueratio
FROM store_sales
JOIN item ON ss_item_sk = i_item_sk
JOIN date_dim ON ss_sold_date_sk = d_date_sk
WHERE i_category IN ('Sports', 'Books', 'Home')
  AND d_date BETWEEN '1999-02-22' AND DATE_ADD('1999-02-22', 30)
GROUP BY
  i_item_id,
  i_item_desc,
  i_category,
  i_class,
  i_current_price
ORDER BY
  i_category,
  i_class,
  i_item_id,
  i_item_desc,
  revenueratio
LIMIT 100
