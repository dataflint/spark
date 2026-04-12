-- TPC-DS Query 30
-- Find customers who have web returns exceeding 120% of the average
-- web returns for their state.
WITH customer_total_return AS (
  SELECT
    wr_returning_customer_sk AS ctr_customer_sk,
    ca_state AS ctr_state,
    SUM(wr_return_amt) AS ctr_total_return
  FROM web_returns
  JOIN date_dim ON wr_returned_date_sk = d_date_sk
  JOIN customer_address ON wr_returning_addr_sk = ca_address_sk
  WHERE d_year = 2002
  GROUP BY wr_returning_customer_sk, ca_state
)
SELECT
  c_customer_id,
  c_salutation,
  c_first_name,
  c_last_name,
  c_preferred_cust_flag,
  c_birth_day,
  c_birth_month,
  c_birth_year,
  c_birth_country,
  c_login,
  c_email_address,
  c_last_review_date_sk,
  ctr_total_return
FROM customer_total_return ctr1
JOIN customer_address ON ca_address_sk = c_current_addr_sk
JOIN customer ON ctr1.ctr_customer_sk = c_customer_sk
WHERE ctr1.ctr_total_return > (
  SELECT AVG(ctr_total_return) * 1.2
  FROM customer_total_return ctr2
  WHERE ctr1.ctr_state = ctr2.ctr_state
)
AND ca_state = 'GA'
ORDER BY c_customer_id, c_salutation, c_first_name, c_last_name,
  c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year,
  c_birth_country, c_login, c_email_address, c_last_review_date_sk,
  ctr_total_return
LIMIT 100
