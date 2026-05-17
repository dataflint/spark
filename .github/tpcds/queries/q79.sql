-- TPC-DS Query 79
-- Store sales profit by customer and ticket number
SELECT
  c_last_name,
  c_first_name,
  SUBSTR(s_city, 1, 30) AS city,
  ss_ticket_number,
  amt,
  profit
FROM (
  SELECT
    ss_ticket_number,
    ss_customer_sk,
    store.s_city,
    SUM(ss_coupon_amt) AS amt,
    SUM(ss_net_profit) AS profit
  FROM store_sales
  JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
  JOIN store ON store_sales.ss_store_sk = store.s_store_sk
  JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
  WHERE (household_demographics.hd_dep_count = 6
         OR household_demographics.hd_vehicle_count > 2)
    AND date_dim.d_dow = 1
    AND date_dim.d_year IN (1999, 2000, 2001)
    AND store.s_number_employees BETWEEN 200 AND 295
  GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, store.s_city
) ms
JOIN customer ON ss_customer_sk = c_customer_sk
ORDER BY c_last_name, c_first_name, city, profit
LIMIT 100
