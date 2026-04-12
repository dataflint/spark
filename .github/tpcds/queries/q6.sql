-- TPC-DS Query 6
-- List all the states with at least 10 customers who during a given month
-- bought items with the price tag at least 20% higher than the average price
-- of items in the same category.
SELECT a.ca_state AS state, COUNT(*) AS cnt
FROM customer_address a
JOIN customer c ON a.ca_address_sk = c.c_current_addr_sk
JOIN store_sales s ON c.c_customer_sk = s.ss_customer_sk
JOIN date_dim d ON s.ss_sold_date_sk = d.d_date_sk
JOIN item i ON s.ss_item_sk = i.i_item_sk
WHERE d.d_month_seq = (
  SELECT DISTINCT d_month_seq
  FROM date_dim
  WHERE d_year = 2001 AND d_moy = 1
)
AND i.i_current_price > 1.2 * (
  SELECT AVG(j.i_current_price)
  FROM item j
  WHERE j.i_category = i.i_category
)
GROUP BY a.ca_state
HAVING COUNT(*) >= 10
ORDER BY cnt, a.ca_state
LIMIT 100
