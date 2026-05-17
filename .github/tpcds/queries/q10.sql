-- TPC-DS Query 10
-- Count the customers with specific demographics who have purchased from
-- at least one of store, catalog, or web in a given year and month.
SELECT
  cd_gender,
  cd_marital_status,
  cd_education_status,
  COUNT(*) AS cnt1,
  cd_purchase_estimate,
  COUNT(*) AS cnt2,
  cd_credit_rating,
  COUNT(*) AS cnt3,
  cd_dep_count,
  COUNT(*) AS cnt4,
  cd_dep_employed_count,
  COUNT(*) AS cnt5,
  cd_dep_college_count,
  COUNT(*) AS cnt6
FROM customer c
JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
JOIN customer_demographics cd ON c.c_current_cdemo_sk = cd.cd_demo_sk
WHERE ca.ca_county IN ('Rush County', 'Toole County', 'Jefferson County', 'Dona Ana County', 'La Porte County')
  AND EXISTS (
    SELECT 1 FROM store_sales ss
    JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
    WHERE c.c_customer_sk = ss.ss_customer_sk
      AND d.d_year = 2002 AND d.d_moy BETWEEN 1 AND 4
  )
  AND (
    EXISTS (
      SELECT 1 FROM web_sales ws
      JOIN date_dim d ON ws.ws_sold_date_sk = d.d_date_sk
      WHERE c.c_customer_sk = ws.ws_bill_customer_sk
        AND d.d_year = 2002 AND d.d_moy BETWEEN 1 AND 4
    )
    OR EXISTS (
      SELECT 1 FROM catalog_sales cs
      JOIN date_dim d ON cs.cs_sold_date_sk = d.d_date_sk
      WHERE c.c_customer_sk = cs.cs_bill_customer_sk
        AND d.d_year = 2002 AND d.d_moy BETWEEN 1 AND 4
    )
  )
GROUP BY
  cd_gender, cd_marital_status, cd_education_status,
  cd_purchase_estimate, cd_credit_rating, cd_dep_count,
  cd_dep_employed_count, cd_dep_college_count
ORDER BY
  cd_gender, cd_marital_status, cd_education_status,
  cd_purchase_estimate, cd_credit_rating, cd_dep_count,
  cd_dep_employed_count, cd_dep_college_count
LIMIT 100
