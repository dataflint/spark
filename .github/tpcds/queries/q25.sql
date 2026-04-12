-- TPC-DS Query 25
-- For certain items sold in stores and returned and then re-sold through catalog,
-- compute the aggregated sales and returns.
SELECT
  i_item_id,
  i_item_desc,
  s_store_id,
  s_store_name,
  SUM(ss_net_profit) AS store_sales_profit,
  SUM(sr_net_loss) AS store_returns_loss,
  SUM(cs_net_profit) AS catalog_sales_profit
FROM store_sales
JOIN store_returns ON ss_customer_sk = sr_customer_sk AND ss_item_sk = sr_item_sk AND ss_ticket_number = sr_ticket_number
JOIN catalog_sales ON sr_customer_sk = cs_bill_customer_sk AND sr_item_sk = cs_item_sk
JOIN date_dim d1 ON ss_sold_date_sk = d1.d_date_sk
JOIN date_dim d2 ON sr_returned_date_sk = d2.d_date_sk
JOIN date_dim d3 ON cs_sold_date_sk = d3.d_date_sk
JOIN store ON s_store_sk = ss_store_sk
JOIN item ON i_item_sk = ss_item_sk
WHERE d1.d_moy = 4
  AND d1.d_year = 2001
  AND d2.d_moy BETWEEN 4 AND 10
  AND d2.d_year = 2001
  AND d3.d_moy BETWEEN 4 AND 10
  AND d3.d_year = 2001
GROUP BY i_item_id, i_item_desc, s_store_id, s_store_name
ORDER BY i_item_id, i_item_desc, s_store_id, s_store_name
LIMIT 100
