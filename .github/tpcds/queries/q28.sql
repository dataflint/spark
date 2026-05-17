-- TPC-DS Query 28
-- Compute various statistics on store sales prices for different list price ranges.
SELECT *
FROM (
  SELECT
    AVG(ss_list_price) AS b1_lp,
    COUNT(ss_list_price) AS b1_cnt,
    COUNT(DISTINCT ss_list_price) AS b1_cntd
  FROM store_sales
  WHERE ss_quantity BETWEEN 0 AND 5
    AND (ss_list_price BETWEEN 8 AND 18
      OR ss_coupon_amt BETWEEN 459 AND 1459
      OR ss_wholesale_cost BETWEEN 57 AND 77)
) b1,
(
  SELECT
    AVG(ss_list_price) AS b2_lp,
    COUNT(ss_list_price) AS b2_cnt,
    COUNT(DISTINCT ss_list_price) AS b2_cntd
  FROM store_sales
  WHERE ss_quantity BETWEEN 6 AND 10
    AND (ss_list_price BETWEEN 90 AND 100
      OR ss_coupon_amt BETWEEN 2323 AND 3323
      OR ss_wholesale_cost BETWEEN 31 AND 51)
) b2,
(
  SELECT
    AVG(ss_list_price) AS b3_lp,
    COUNT(ss_list_price) AS b3_cnt,
    COUNT(DISTINCT ss_list_price) AS b3_cntd
  FROM store_sales
  WHERE ss_quantity BETWEEN 11 AND 15
    AND (ss_list_price BETWEEN 142 AND 152
      OR ss_coupon_amt BETWEEN 12214 AND 13214
      OR ss_wholesale_cost BETWEEN 79 AND 99)
) b3,
(
  SELECT
    AVG(ss_list_price) AS b4_lp,
    COUNT(ss_list_price) AS b4_cnt,
    COUNT(DISTINCT ss_list_price) AS b4_cntd
  FROM store_sales
  WHERE ss_quantity BETWEEN 16 AND 20
    AND (ss_list_price BETWEEN 135 AND 145
      OR ss_coupon_amt BETWEEN 6071 AND 7071
      OR ss_wholesale_cost BETWEEN 38 AND 58)
) b4,
(
  SELECT
    AVG(ss_list_price) AS b5_lp,
    COUNT(ss_list_price) AS b5_cnt,
    COUNT(DISTINCT ss_list_price) AS b5_cntd
  FROM store_sales
  WHERE ss_quantity BETWEEN 21 AND 25
    AND (ss_list_price BETWEEN 122 AND 132
      OR ss_coupon_amt BETWEEN 836 AND 1836
      OR ss_wholesale_cost BETWEEN 17 AND 37)
) b5,
(
  SELECT
    AVG(ss_list_price) AS b6_lp,
    COUNT(ss_list_price) AS b6_cnt,
    COUNT(DISTINCT ss_list_price) AS b6_cntd
  FROM store_sales
  WHERE ss_quantity BETWEEN 26 AND 30
    AND (ss_list_price BETWEEN 154 AND 164
      OR ss_coupon_amt BETWEEN 7326 AND 8326
      OR ss_wholesale_cost BETWEEN 7 AND 27)
) b6
LIMIT 100
