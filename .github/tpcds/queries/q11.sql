-- TPC-DS Query 11
-- Find customers whose increase in web sales was greater than their increase in store sales
-- from one year to the next.
WITH year_total AS (
  SELECT
    c_customer_id AS customer_id,
    c_first_name AS customer_first_name,
    c_last_name AS customer_last_name,
    c_preferred_cust_flag AS customer_preferred_cust_flag,
    c_birth_country AS customer_birth_country,
    c_login AS customer_login,
    c_email_address AS customer_email_address,
    d_year AS dyear,
    SUM(ss_ext_list_price - ss_ext_discount_amt) AS year_total_amt,
    's' AS sale_type
  FROM customer
  JOIN store_sales ON c_customer_sk = ss_customer_sk
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  WHERE d_year IN (2001, 2002)
  GROUP BY
    c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag,
    c_birth_country, c_login, c_email_address, d_year
  UNION ALL
  SELECT
    c_customer_id AS customer_id,
    c_first_name AS customer_first_name,
    c_last_name AS customer_last_name,
    c_preferred_cust_flag AS customer_preferred_cust_flag,
    c_birth_country AS customer_birth_country,
    c_login AS customer_login,
    c_email_address AS customer_email_address,
    d_year AS dyear,
    SUM(ws_ext_list_price - ws_ext_discount_amt) AS year_total_amt,
    'w' AS sale_type
  FROM customer
  JOIN web_sales ON c_customer_sk = ws_bill_customer_sk
  JOIN date_dim ON ws_sold_date_sk = d_date_sk
  WHERE d_year IN (2001, 2002)
  GROUP BY
    c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag,
    c_birth_country, c_login, c_email_address, d_year
)
SELECT
  t_s_secyear.customer_id,
  t_s_secyear.customer_first_name,
  t_s_secyear.customer_last_name,
  t_s_secyear.customer_preferred_cust_flag
FROM year_total t_s_firstyear
JOIN year_total t_s_secyear
  ON t_s_firstyear.customer_id = t_s_secyear.customer_id
  AND t_s_firstyear.sale_type = 's'
  AND t_s_secyear.sale_type = 's'
  AND t_s_firstyear.dyear = 2001
  AND t_s_secyear.dyear = 2002
JOIN year_total t_w_firstyear
  ON t_s_firstyear.customer_id = t_w_firstyear.customer_id
  AND t_w_firstyear.sale_type = 'w'
  AND t_w_firstyear.dyear = 2001
JOIN year_total t_w_secyear
  ON t_s_firstyear.customer_id = t_w_secyear.customer_id
  AND t_w_secyear.sale_type = 'w'
  AND t_w_secyear.dyear = 2002
WHERE t_s_firstyear.year_total_amt > 0
  AND t_w_firstyear.year_total_amt > 0
  AND CASE WHEN t_w_firstyear.year_total_amt > 0
    THEN t_w_secyear.year_total_amt / t_w_firstyear.year_total_amt
    ELSE 0.0 END
    > CASE WHEN t_s_firstyear.year_total_amt > 0
    THEN t_s_secyear.year_total_amt / t_s_firstyear.year_total_amt
    ELSE 0.0 END
ORDER BY
  t_s_secyear.customer_id,
  t_s_secyear.customer_first_name,
  t_s_secyear.customer_last_name,
  t_s_secyear.customer_preferred_cust_flag
LIMIT 100
