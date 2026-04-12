-- TPC-DS Query 16
-- Report number of orders, total shipping costs, and net profits
-- from catalog sales fulfilled from warehouses in a given state
-- where the order was placed but not returned.
SELECT
  COUNT(DISTINCT cs_order_number) AS order_count,
  SUM(cs_ext_ship_cost) AS total_shipping_cost,
  SUM(cs_net_profit) AS total_net_profit
FROM catalog_sales cs1
JOIN date_dim ON cs_sold_date_sk = d_date_sk
JOIN customer_address ON cs_ship_addr_sk = ca_address_sk
JOIN call_center ON cs_call_center_sk = cc_call_center_sk
WHERE d_date BETWEEN '2002-02-01' AND '2002-04-02'
  AND ca_state = 'GA'
  AND cc_county IN ('Williamson County', 'Williamson County', 'Williamson County',
                    'Williamson County', 'Williamson County')
  AND EXISTS (
    SELECT 1 FROM catalog_sales cs2
    WHERE cs1.cs_order_number = cs2.cs_order_number
      AND cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk
  )
  AND NOT EXISTS (
    SELECT 1 FROM catalog_returns cr1
    WHERE cs1.cs_order_number = cr1.cr_order_number
  )
ORDER BY order_count
LIMIT 100
