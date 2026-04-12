-- TPC-DS Query 41
-- Find items with specific manufacturing characteristics that are sold at certain price points.
SELECT DISTINCT i_product_name
FROM item i1
WHERE i_manufact_id BETWEEN 738 AND 738 + 40
  AND (
    SELECT COUNT(*) AS item_cnt
    FROM item
    WHERE (i_manufact = i1.i_manufact
      AND ((i_category = 'Women' AND (i_color = 'powder' OR i_color = 'khaki')
            AND (i_units = 'Ounce' OR i_units = 'Oz')
            AND (i_size = 'medium' OR i_size = 'extra large'))
        OR (i_category = 'Women' AND (i_color = 'brown' OR i_color = 'honeydew')
            AND (i_units = 'Bunch' OR i_units = 'Ton')
            AND (i_size = 'N/A' OR i_size = 'small'))
        OR (i_category = 'Men' AND (i_color = 'floral' OR i_color = 'deep')
            AND (i_units = 'N/A' OR i_units = 'Dozen')
            AND (i_size = 'petite' OR i_size = 'large'))
        OR (i_category = 'Men' AND (i_color = 'light' OR i_color = 'cornflower')
            AND (i_units = 'Box' OR i_units = 'Pound')
            AND (i_size = 'medium' OR i_size = 'extra large'))))
  ) > 0
ORDER BY i_product_name
LIMIT 100
