-- TPC-DS Query 50
-- Report the number of store returns by return time buckets for specific stores.
SELECT
  s_store_name,
  s_company_id,
  s_street_number,
  s_street_name,
  s_street_type,
  s_suite_number,
  s_city,
  s_county,
  s_state,
  s_zip,
  SUM(CASE WHEN (sr_returned_date_sk - ss_sold_date_sk <= 30) THEN 1 ELSE 0 END) AS `30 days`,
  SUM(CASE WHEN (sr_returned_date_sk - ss_sold_date_sk > 30)
    AND (sr_returned_date_sk - ss_sold_date_sk <= 60) THEN 1 ELSE 0 END) AS `31-60 days`,
  SUM(CASE WHEN (sr_returned_date_sk - ss_sold_date_sk > 60)
    AND (sr_returned_date_sk - ss_sold_date_sk <= 90) THEN 1 ELSE 0 END) AS `61-90 days`,
  SUM(CASE WHEN (sr_returned_date_sk - ss_sold_date_sk > 90)
    AND (sr_returned_date_sk - ss_sold_date_sk <= 120) THEN 1 ELSE 0 END) AS `91-120 days`,
  SUM(CASE WHEN (sr_returned_date_sk - ss_sold_date_sk > 120) THEN 1 ELSE 0 END) AS `>120 days`
FROM store_sales
JOIN store_returns ON ss_ticket_number = sr_ticket_number
  AND ss_item_sk = sr_item_sk
  AND ss_customer_sk = sr_customer_sk
JOIN store ON ss_store_sk = s_store_sk
JOIN date_dim d1 ON sr_returned_date_sk = d1.d_date_sk
WHERE d1.d_year = 2001
  AND d1.d_moy = 8
GROUP BY
  s_store_name, s_company_id, s_street_number, s_street_name, s_street_type,
  s_suite_number, s_city, s_county, s_state, s_zip
ORDER BY
  s_store_name, s_company_id, s_street_number, s_street_name, s_street_type,
  s_suite_number, s_city, s_county, s_state, s_zip
LIMIT 100
