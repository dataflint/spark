-- TPC-DS Query 82
-- Items with specific inventory levels, price range, and brand
SELECT
  i_item_id,
  i_item_desc,
  i_current_price
FROM item
JOIN inventory ON inv_item_sk = i_item_sk
JOIN date_dim ON d_date_sk = inv_date_sk
JOIN store_sales ON ss_item_sk = i_item_sk
WHERE i_current_price BETWEEN 62 AND 62 + 30
  AND d_date BETWEEN '2000-05-25' AND DATE_ADD('2000-05-25', 60)
  AND inv_quantity_on_hand BETWEEN 100 AND 500
  AND i_manufact_id IN (129, 270, 821, 423)
GROUP BY i_item_id, i_item_desc, i_current_price
ORDER BY i_item_id
LIMIT 100
