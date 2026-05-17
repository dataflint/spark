-- TPC-DS Query 5
-- Report sales, returns, and profit for store, catalog, and web channels
-- for a particular 14-day period.
WITH ssr AS (
  SELECT
    s_store_id,
    SUM(ss_ext_sales_price) AS sales,
    SUM(ss_net_profit) AS profit,
    SUM(sr_return_amt) AS returns_amt,
    SUM(sr_net_loss) AS profit_loss
  FROM store_sales
  LEFT JOIN store_returns
    ON ss_item_sk = sr_item_sk AND ss_ticket_number = sr_ticket_number
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  JOIN store ON ss_store_sk = s_store_sk
  WHERE d_date BETWEEN '2000-08-23' AND '2000-09-06'
  GROUP BY s_store_id
),
csr AS (
  SELECT
    cp_catalog_page_id,
    SUM(cs_ext_sales_price) AS sales,
    SUM(cs_net_profit) AS profit,
    SUM(cr_return_amount) AS returns_amt,
    SUM(cr_net_loss) AS profit_loss
  FROM catalog_sales
  LEFT JOIN catalog_returns
    ON cs_item_sk = cr_item_sk AND cs_order_number = cr_order_number
  JOIN date_dim ON cs_sold_date_sk = d_date_sk
  JOIN catalog_page ON cs_catalog_page_sk = cp_catalog_page_sk
  WHERE d_date BETWEEN '2000-08-23' AND '2000-09-06'
  GROUP BY cp_catalog_page_id
),
wsr AS (
  SELECT
    web_site_id,
    SUM(ws_ext_sales_price) AS sales,
    SUM(ws_net_profit) AS profit,
    SUM(wr_return_amt) AS returns_amt,
    SUM(wr_net_loss) AS profit_loss
  FROM web_sales
  LEFT JOIN web_returns
    ON ws_item_sk = wr_item_sk AND ws_order_number = wr_order_number
  JOIN date_dim ON ws_sold_date_sk = d_date_sk
  JOIN web_site ON ws_web_site_sk = web_site_sk
  WHERE d_date BETWEEN '2000-08-23' AND '2000-09-06'
  GROUP BY web_site_id
),
results AS (
  SELECT
    'store channel' AS channel,
    CONCAT('store', s_store_id) AS id,
    sales, returns_amt, profit - profit_loss AS profit
  FROM ssr
  UNION ALL
  SELECT
    'catalog channel' AS channel,
    CONCAT('catalog_page', cp_catalog_page_id) AS id,
    sales, returns_amt, profit - profit_loss AS profit
  FROM csr
  UNION ALL
  SELECT
    'web channel' AS channel,
    CONCAT('web_site', web_site_id) AS id,
    sales, returns_amt, profit - profit_loss AS profit
  FROM wsr
)
SELECT
  channel,
  id,
  SUM(sales) AS sales,
  SUM(returns_amt) AS returns_amt,
  SUM(profit) AS profit
FROM results
GROUP BY channel, id
ORDER BY channel, id
LIMIT 100
