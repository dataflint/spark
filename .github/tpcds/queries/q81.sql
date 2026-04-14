-- TPC-DS Query 81
-- Customers with catalog returns exceeding average by state
WITH customer_total_return AS (
  SELECT
    cr_returning_customer_sk AS ctr_customer_sk,
    ca_state AS ctr_state,
    SUM(cr_return_amt_inc_tax) AS ctr_total_return
  FROM catalog_returns
  JOIN date_dim ON cr_returned_date_sk = d_date_sk
  JOIN customer_address ON cr_returning_addr_sk = ca_address_sk
  WHERE d_year = 2000
  GROUP BY cr_returning_customer_sk, ca_state
)
SELECT
  c_customer_id,
  c_salutation,
  c_first_name,
  c_last_name,
  ca_street_number,
  ca_street_name,
  ca_street_type,
  ca_suite_number,
  ca_city,
  ca_county,
  ca_state,
  ca_zip,
  ca_country,
  ca_gmt_offset,
  ca_location_type,
  ctr_total_return
FROM customer_total_return ctr1
JOIN customer ON ctr1.ctr_customer_sk = c_customer_sk
JOIN customer_address ON ca_address_sk = c_current_addr_sk
WHERE ctr1.ctr_total_return > (
    SELECT AVG(ctr_total_return) * 1.2
    FROM customer_total_return ctr2
    WHERE ctr1.ctr_state = ctr2.ctr_state
  )
  AND ca_state = 'GA'
ORDER BY
  c_customer_id,
  c_salutation,
  c_first_name,
  c_last_name,
  ca_street_number,
  ca_street_name,
  ca_street_type,
  ca_suite_number,
  ca_city,
  ca_county,
  ca_state,
  ca_zip,
  ca_country,
  ca_gmt_offset,
  ca_location_type,
  ctr_total_return
LIMIT 100
