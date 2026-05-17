-- TPC-DS Query 18
-- Compute average catalog sales for demographic and date characteristics
-- with rollup over multiple grouping sets.
SELECT
  i_item_id,
  ca_country,
  ca_state,
  ca_county,
  AVG(CAST(cs_quantity AS DECIMAL(12,2))) AS agg1,
  AVG(CAST(cs_list_price AS DECIMAL(12,2))) AS agg2,
  AVG(CAST(cs_coupon_amt AS DECIMAL(12,2))) AS agg3,
  AVG(CAST(cs_sales_price AS DECIMAL(12,2))) AS agg4,
  AVG(CAST(cs_net_profit AS DECIMAL(12,2))) AS agg5,
  AVG(CAST(c_birth_year AS DECIMAL(12,2))) AS agg6,
  AVG(CAST(cd_dep_count AS DECIMAL(12,2))) AS agg7
FROM catalog_sales
JOIN customer_demographics ON cs_bill_cdemo_sk = cd_demo_sk
JOIN customer ON cs_bill_customer_sk = c_customer_sk
JOIN customer_address ON c_current_addr_sk = ca_address_sk
JOIN date_dim ON cs_sold_date_sk = d_date_sk
JOIN item ON cs_item_sk = i_item_sk
WHERE cd_gender = 'F'
  AND cd_education_status = 'Unknown'
  AND c_birth_month IN (1, 6, 8, 9, 12, 2)
  AND d_year = 1998
  AND ca_state IN ('MS', 'IN', 'ND', 'OK', 'NM', 'VA', 'MS')
GROUP BY ROLLUP(i_item_id, ca_country, ca_state, ca_county)
ORDER BY ca_country, ca_state, ca_county, i_item_id
LIMIT 100
