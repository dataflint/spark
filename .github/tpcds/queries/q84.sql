-- TPC-DS Query 84
-- Customer demographics join with income band and store returns
SELECT
  c_customer_id AS customer_id,
  CONCAT(COALESCE(c_last_name, ''), ', ', COALESCE(c_first_name, '')) AS customername
FROM customer
JOIN customer_address ON c_current_addr_sk = ca_address_sk
JOIN customer_demographics ON cd_demo_sk = c_current_cdemo_sk
JOIN household_demographics ON hd_demo_sk = c_current_hdemo_sk
JOIN income_band ON ib_income_band_sk = hd_income_band_sk
JOIN store_returns ON sr_cdemo_sk = cd_demo_sk
WHERE ca_city = 'Edgewood'
  AND cd_marital_status = 'U'
  AND cd_education_status = 'Unknown'
  AND ib_lower_bound >= 38128
  AND ib_upper_bound <= 38128 + 50000
GROUP BY c_customer_id, c_last_name, c_first_name
ORDER BY customer_id
LIMIT 100
