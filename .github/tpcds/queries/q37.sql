-- TPC-DS Query 37
-- List items that were sold through catalog within a specific date range and
-- price range and had inventory in a certain quantity range.
SELECT i_item_id, i_item_desc, i_current_price
FROM item
JOIN inventory ON inv_item_sk = i_item_sk
JOIN date_dim ON d_date_sk = inv_date_sk
JOIN catalog_sales ON cs_item_sk = i_item_sk
WHERE i_current_price BETWEEN 68 AND 68 + 30
  AND d_date BETWEEN '2000-02-01' AND DATE_ADD('2000-02-01', 60)
  AND inv_quantity_on_hand BETWEEN 100 AND 500
GROUP BY i_item_id, i_item_desc, i_current_price
ORDER BY i_item_id
LIMIT 100
