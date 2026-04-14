-- TPC-DS Query 2
-- Report the ratios of weekly web and catalog sales increases from one year to the next.
WITH wscs AS (
  SELECT
    d_week_seq,
    SUM(CASE WHEN d_dow = 0 THEN sales_price ELSE 0 END) AS sun_sales,
    SUM(CASE WHEN d_dow = 1 THEN sales_price ELSE 0 END) AS mon_sales,
    SUM(CASE WHEN d_dow = 2 THEN sales_price ELSE 0 END) AS tue_sales,
    SUM(CASE WHEN d_dow = 3 THEN sales_price ELSE 0 END) AS wed_sales,
    SUM(CASE WHEN d_dow = 4 THEN sales_price ELSE 0 END) AS thu_sales,
    SUM(CASE WHEN d_dow = 5 THEN sales_price ELSE 0 END) AS fri_sales,
    SUM(CASE WHEN d_dow = 6 THEN sales_price ELSE 0 END) AS sat_sales
  FROM (
    SELECT ws_sold_date_sk AS sold_date_sk, ws_ext_sales_price AS sales_price
    FROM web_sales
    UNION ALL
    SELECT cs_sold_date_sk AS sold_date_sk, cs_ext_sales_price AS sales_price
    FROM catalog_sales
  ) x
  JOIN date_dim ON sold_date_sk = d_date_sk
  GROUP BY d_week_seq
),
y AS (
  SELECT
    wscs.d_week_seq,
    sun_sales,
    mon_sales,
    tue_sales,
    wed_sales,
    thu_sales,
    fri_sales,
    sat_sales
  FROM wscs
  JOIN date_dim dd ON wscs.d_week_seq = dd.d_week_seq
  WHERE dd.d_year = 2001
),
z AS (
  SELECT
    wscs.d_week_seq,
    sun_sales,
    mon_sales,
    tue_sales,
    wed_sales,
    thu_sales,
    fri_sales,
    sat_sales
  FROM wscs
  JOIN date_dim dd ON wscs.d_week_seq = dd.d_week_seq
  WHERE dd.d_year = 2001 + 1
)
SELECT
  y.d_week_seq AS y_week_seq,
  ROUND(z.sun_sales / NULLIF(y.sun_sales, 0), 2) AS sun_ratio,
  ROUND(z.mon_sales / NULLIF(y.mon_sales, 0), 2) AS mon_ratio,
  ROUND(z.tue_sales / NULLIF(y.tue_sales, 0), 2) AS tue_ratio,
  ROUND(z.wed_sales / NULLIF(y.wed_sales, 0), 2) AS wed_ratio,
  ROUND(z.thu_sales / NULLIF(y.thu_sales, 0), 2) AS thu_ratio,
  ROUND(z.fri_sales / NULLIF(y.fri_sales, 0), 2) AS fri_ratio,
  ROUND(z.sat_sales / NULLIF(y.sat_sales, 0), 2) AS sat_ratio
FROM y
JOIN z ON y.d_week_seq = z.d_week_seq - 53
ORDER BY y.d_week_seq
LIMIT 100
