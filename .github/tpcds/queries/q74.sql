-- TPC-DS Query 74
-- Year-over-year comparison of store and web sales by customer
WITH year_total AS (
  SELECT
    c_customer_id AS customer_id,
    c_first_name AS customer_first_name,
    c_last_name AS customer_last_name,
    d_year AS year,
    SUM(ss_net_paid) AS year_total,
    's' AS sale_type
  FROM customer
  JOIN store_sales ON c_customer_sk = ss_customer_sk
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  WHERE d_year IN (2001, 2002)
  GROUP BY c_customer_id, c_first_name, c_last_name, d_year

  UNION ALL

  SELECT
    c_customer_id AS customer_id,
    c_first_name AS customer_first_name,
    c_last_name AS customer_last_name,
    d_year AS year,
    SUM(ws_net_paid) AS year_total,
    'w' AS sale_type
  FROM customer
  JOIN web_sales ON c_customer_sk = ws_bill_customer_sk
  JOIN date_dim ON ws_sold_date_sk = d_date_sk
  WHERE d_year IN (2001, 2002)
  GROUP BY c_customer_id, c_first_name, c_last_name, d_year
)
SELECT
  t_s_secyear.customer_id,
  t_s_secyear.customer_first_name,
  t_s_secyear.customer_last_name
FROM year_total t_s_firstyear
JOIN year_total t_s_secyear
  ON t_s_secyear.customer_id = t_s_firstyear.customer_id
JOIN year_total t_w_firstyear
  ON t_s_firstyear.customer_id = t_w_firstyear.customer_id
JOIN year_total t_w_secyear
  ON t_s_firstyear.customer_id = t_w_secyear.customer_id
WHERE t_s_firstyear.sale_type = 's'
  AND t_s_firstyear.year = 2001
  AND t_s_secyear.sale_type = 's'
  AND t_s_secyear.year = 2002
  AND t_w_firstyear.sale_type = 'w'
  AND t_w_firstyear.year = 2001
  AND t_w_secyear.sale_type = 'w'
  AND t_w_secyear.year = 2002
  AND t_s_firstyear.year_total > 0
  AND t_w_firstyear.year_total > 0
  AND CASE WHEN t_w_firstyear.year_total > 0
           THEN t_w_secyear.year_total / t_w_firstyear.year_total
           ELSE NULL END
      > CASE WHEN t_s_firstyear.year_total > 0
             THEN t_s_secyear.year_total / t_s_firstyear.year_total
             ELSE NULL END
ORDER BY 1, 2, 3
LIMIT 100
