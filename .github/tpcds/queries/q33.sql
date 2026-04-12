-- TPC-DS Query 33
-- Report the total extended sales price for items of specific manufacturers
-- sold through store, catalog, and web in a given month and year,
-- shipped to customers in a specific GMT offset.
WITH ss AS (
  SELECT
    i_manufact_id,
    SUM(ss_ext_sales_price) AS total_sales
  FROM store_sales
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  JOIN customer_address ON ss_addr_sk = ca_address_sk
  JOIN item ON ss_item_sk = i_item_sk
  WHERE i_manufact_id IN (
    SELECT i_manufact_id
    FROM item
    WHERE i_category IN ('Books')
  )
  AND d_year = 1998 AND d_moy = 5
  AND ca_gmt_offset = -5
  GROUP BY i_manufact_id
),
cs AS (
  SELECT
    i_manufact_id,
    SUM(cs_ext_sales_price) AS total_sales
  FROM catalog_sales
  JOIN date_dim ON cs_sold_date_sk = d_date_sk
  JOIN customer_address ON cs_bill_addr_sk = ca_address_sk
  JOIN item ON cs_item_sk = i_item_sk
  WHERE i_manufact_id IN (
    SELECT i_manufact_id
    FROM item
    WHERE i_category IN ('Books')
  )
  AND d_year = 1998 AND d_moy = 5
  AND ca_gmt_offset = -5
  GROUP BY i_manufact_id
),
ws AS (
  SELECT
    i_manufact_id,
    SUM(ws_ext_sales_price) AS total_sales
  FROM web_sales
  JOIN date_dim ON ws_sold_date_sk = d_date_sk
  JOIN customer_address ON ws_bill_addr_sk = ca_address_sk
  JOIN item ON ws_item_sk = i_item_sk
  WHERE i_manufact_id IN (
    SELECT i_manufact_id
    FROM item
    WHERE i_category IN ('Books')
  )
  AND d_year = 1998 AND d_moy = 5
  AND ca_gmt_offset = -5
  GROUP BY i_manufact_id
)
SELECT
  i_manufact_id,
  SUM(total_sales) AS total_sales
FROM (
  SELECT * FROM ss
  UNION ALL
  SELECT * FROM cs
  UNION ALL
  SELECT * FROM ws
) tmp
GROUP BY i_manufact_id
ORDER BY total_sales
LIMIT 100
