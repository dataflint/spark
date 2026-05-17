-- TPC-DS Query 29
-- For items sold in stores, returned, and then sold through catalog,
-- report the item, store, and various quantity statistics.
SELECT
  i_item_id,
  i_item_desc,
  s_store_id,
  s_store_name,
  AVG(ss_quantity) AS store_sales_quantity,
  AVG(sr_return_quantity) AS store_returns_quantity,
  AVG(cs_quantity) AS catalog_sales_quantity
FROM store_sales
JOIN store_returns ON ss_customer_sk = sr_customer_sk AND ss_item_sk = sr_item_sk AND ss_ticket_number = sr_ticket_number
JOIN catalog_sales ON sr_customer_sk = cs_bill_customer_sk AND sr_item_sk = cs_item_sk
JOIN date_dim d1 ON ss_sold_date_sk = d1.d_date_sk
JOIN date_dim d2 ON sr_returned_date_sk = d2.d_date_sk
JOIN date_dim d3 ON cs_sold_date_sk = d3.d_date_sk
JOIN store ON s_store_sk = ss_store_sk
JOIN item ON i_item_sk = ss_item_sk
WHERE d1.d_moy = 9
  AND d1.d_year = 1999
  AND d2.d_moy BETWEEN 9 AND 12
  AND d2.d_year = 1999
  AND d3.d_year IN (1999, 2000, 2001)
GROUP BY i_item_id, i_item_desc, s_store_id, s_store_name
ORDER BY i_item_id, i_item_desc, s_store_id, s_store_name
LIMIT 100
