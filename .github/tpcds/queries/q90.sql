-- TPC-DS Query 90
-- Ratio of web sales during morning vs evening hours
SELECT
  CAST(amc AS DECIMAL(15,4)) / CAST(pmc AS DECIMAL(15,4)) AS am_pm_ratio
FROM (
  SELECT COUNT(*) AS amc
  FROM web_sales
  JOIN household_demographics ON ws_ship_hdemo_sk = hd_demo_sk
  JOIN time_dim ON ws_sold_time_sk = t_time_sk
  JOIN web_page ON ws_web_page_sk = wp_web_page_sk
  WHERE t_hour BETWEEN 8 AND 8 + 1
    AND hd_dep_count = 6
    AND wp_char_count BETWEEN 5000 AND 5200
) at_val,
(
  SELECT COUNT(*) AS pmc
  FROM web_sales
  JOIN household_demographics ON ws_ship_hdemo_sk = hd_demo_sk
  JOIN time_dim ON ws_sold_time_sk = t_time_sk
  JOIN web_page ON ws_web_page_sk = wp_web_page_sk
  WHERE t_hour BETWEEN 19 AND 19 + 1
    AND hd_dep_count = 6
    AND wp_char_count BETWEEN 5000 AND 5200
) pt_val
ORDER BY am_pm_ratio
LIMIT 100
