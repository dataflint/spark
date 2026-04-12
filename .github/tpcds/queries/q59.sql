-- TPC-DS Query 59
-- Compute weekly store sales and compare with the same week in the prior year
-- using a ratio of current to prior year sales by day of week.
WITH wss AS (
  SELECT
    d_week_seq,
    ss_store_sk,
    SUM(CASE WHEN d_day_name = 'Sunday' THEN ss_sales_price ELSE NULL END) AS sun_sales,
    SUM(CASE WHEN d_day_name = 'Monday' THEN ss_sales_price ELSE NULL END) AS mon_sales,
    SUM(CASE WHEN d_day_name = 'Tuesday' THEN ss_sales_price ELSE NULL END) AS tue_sales,
    SUM(CASE WHEN d_day_name = 'Wednesday' THEN ss_sales_price ELSE NULL END) AS wed_sales,
    SUM(CASE WHEN d_day_name = 'Thursday' THEN ss_sales_price ELSE NULL END) AS thu_sales,
    SUM(CASE WHEN d_day_name = 'Friday' THEN ss_sales_price ELSE NULL END) AS fri_sales,
    SUM(CASE WHEN d_day_name = 'Saturday' THEN ss_sales_price ELSE NULL END) AS sat_sales
  FROM store_sales
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  GROUP BY d_week_seq, ss_store_sk
)
SELECT
  s_store_name AS s_store_name1,
  wss.d_week_seq AS d_week_seq1,
  wss.sun_sales / y.sun_sales AS sun_ratio,
  wss.mon_sales / y.mon_sales AS mon_ratio,
  wss.tue_sales / y.tue_sales AS tue_ratio,
  wss.wed_sales / y.wed_sales AS wed_ratio,
  wss.thu_sales / y.thu_sales AS thu_ratio,
  wss.fri_sales / y.fri_sales AS fri_ratio,
  wss.sat_sales / y.sat_sales AS sat_ratio
FROM wss
JOIN store ON wss.ss_store_sk = s_store_sk
JOIN date_dim d ON wss.d_week_seq = d.d_week_seq
JOIN wss y ON y.ss_store_sk = wss.ss_store_sk AND y.d_week_seq = wss.d_week_seq - 52
WHERE d.d_month_seq BETWEEN 1212 AND 1212 + 11
  AND s_gmt_offset = -5
ORDER BY s_store_name, d_week_seq1
LIMIT 100
