-- TPC-DS Query 91
-- Catalog returns by call center, manager, and customer demographics
SELECT
  cc_call_center_id AS call_center,
  cc_name AS call_center_name,
  cc_manager AS manager,
  SUM(cr_net_loss) AS returns_loss
FROM call_center
JOIN catalog_returns ON cr_call_center_sk = cc_call_center_sk
JOIN date_dim ON cr_returned_date_sk = d_date_sk
JOIN customer ON cr_returning_customer_sk = c_customer_sk
JOIN customer_address ON ca_address_sk = c_current_addr_sk
JOIN customer_demographics ON cd_demo_sk = c_current_cdemo_sk
JOIN household_demographics ON hd_demo_sk = c_current_hdemo_sk
WHERE d_year = 1998
  AND d_moy = 11
  AND (
    (cd_marital_status = 'M' AND cd_education_status = 'Unknown')
    OR (cd_marital_status = 'W' AND cd_education_status = 'Advanced Degree')
  )
  AND hd_buy_potential LIKE 'Unknown%'
  AND ca_gmt_offset = -7
GROUP BY cc_call_center_id, cc_name, cc_manager, cd_marital_status, cd_education_status
ORDER BY returns_loss DESC
LIMIT 100
