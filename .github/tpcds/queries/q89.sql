-- TPC-DS Query 89
-- Monthly store sales that deviate significantly from average
SELECT *
FROM (
  SELECT
    i_category,
    i_class,
    i_brand,
    s_store_name,
    s_company_name,
    d_moy,
    SUM(ss_sales_price) AS sum_sales,
    AVG(SUM(ss_sales_price)) OVER (
      PARTITION BY i_category, i_brand, s_store_name, s_company_name
    ) AS avg_monthly_sales
  FROM item
  JOIN store_sales ON ss_item_sk = i_item_sk
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  JOIN store ON ss_store_sk = s_store_sk
  WHERE d_year = 1999
    AND (
      (i_category IN ('Children', 'Music', 'Home')
       AND i_class IN ('toddlers', 'pop', 'lighting'))
      OR
      (i_category IN ('Jewelry', 'Books', 'Sports')
       AND i_class IN ('costume', 'travel', 'football'))
    )
  GROUP BY
    i_category, i_class, i_brand,
    s_store_name, s_company_name, d_moy
) tmp1
WHERE CASE WHEN avg_monthly_sales <> 0
           THEN ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales
           ELSE NULL END > 0.1
ORDER BY sum_sales - avg_monthly_sales, s_store_name
LIMIT 100
