-- TPC-DS Query 46
-- Report store sales for customers who live in a different city than the store,
-- with specific household demographics and date criteria.
SELECT
  c_last_name,
  c_first_name,
  ca_city,
  bought_city,
  ss_ticket_number,
  amt,
  profit
FROM (
  SELECT
    ss_ticket_number,
    ss_customer_sk,
    ca_city AS bought_city,
    SUM(ss_coupon_amt) AS amt,
    SUM(ss_net_profit) AS profit
  FROM store_sales
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  JOIN store ON ss_store_sk = s_store_sk
  JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk
  JOIN customer_address ON ss_addr_sk = ca_address_sk
  WHERE (hd_dep_count = 4 OR hd_vehicle_count = 3)
    AND d_dow IN (6, 0)
    AND d_year IN (1999, 2000, 2001)
    AND s_city IN ('Midway', 'Fairview', 'Fairview', 'Midway', 'Fairview')
  GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, ca_city
) dn
JOIN customer ON ss_customer_sk = c_customer_sk
JOIN customer_address current_addr ON c_current_addr_sk = current_addr.ca_address_sk
WHERE current_addr.ca_city <> bought_city
ORDER BY c_last_name, c_first_name, ca_city, bought_city, ss_ticket_number
LIMIT 100
