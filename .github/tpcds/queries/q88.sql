-- TPC-DS Query 88
-- Store sales by time of day for specific household demographics
SELECT *
FROM (
  SELECT COUNT(*) AS h8_30_to_9
  FROM store_sales
  JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk
  JOIN time_dim ON ss_sold_time_sk = t_time_sk
  JOIN store ON ss_store_sk = s_store_sk
  WHERE t_hour = 8 AND t_minute >= 30
    AND ((hd_dep_count = 4 AND hd_vehicle_count <= 4 + 2)
      OR (hd_dep_count = 2 AND hd_vehicle_count <= 2 + 2)
      OR (hd_dep_count = 0 AND hd_vehicle_count <= 0 + 2))
    AND s_store_name = 'ese'
) s1,
(
  SELECT COUNT(*) AS h9_to_9_30
  FROM store_sales
  JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk
  JOIN time_dim ON ss_sold_time_sk = t_time_sk
  JOIN store ON ss_store_sk = s_store_sk
  WHERE t_hour = 9 AND t_minute < 30
    AND ((hd_dep_count = 4 AND hd_vehicle_count <= 4 + 2)
      OR (hd_dep_count = 2 AND hd_vehicle_count <= 2 + 2)
      OR (hd_dep_count = 0 AND hd_vehicle_count <= 0 + 2))
    AND s_store_name = 'ese'
) s2,
(
  SELECT COUNT(*) AS h9_30_to_10
  FROM store_sales
  JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk
  JOIN time_dim ON ss_sold_time_sk = t_time_sk
  JOIN store ON ss_store_sk = s_store_sk
  WHERE t_hour = 9 AND t_minute >= 30
    AND ((hd_dep_count = 4 AND hd_vehicle_count <= 4 + 2)
      OR (hd_dep_count = 2 AND hd_vehicle_count <= 2 + 2)
      OR (hd_dep_count = 0 AND hd_vehicle_count <= 0 + 2))
    AND s_store_name = 'ese'
) s3,
(
  SELECT COUNT(*) AS h10_to_10_30
  FROM store_sales
  JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk
  JOIN time_dim ON ss_sold_time_sk = t_time_sk
  JOIN store ON ss_store_sk = s_store_sk
  WHERE t_hour = 10 AND t_minute < 30
    AND ((hd_dep_count = 4 AND hd_vehicle_count <= 4 + 2)
      OR (hd_dep_count = 2 AND hd_vehicle_count <= 2 + 2)
      OR (hd_dep_count = 0 AND hd_vehicle_count <= 0 + 2))
    AND s_store_name = 'ese'
) s4,
(
  SELECT COUNT(*) AS h10_30_to_11
  FROM store_sales
  JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk
  JOIN time_dim ON ss_sold_time_sk = t_time_sk
  JOIN store ON ss_store_sk = s_store_sk
  WHERE t_hour = 10 AND t_minute >= 30
    AND ((hd_dep_count = 4 AND hd_vehicle_count <= 4 + 2)
      OR (hd_dep_count = 2 AND hd_vehicle_count <= 2 + 2)
      OR (hd_dep_count = 0 AND hd_vehicle_count <= 0 + 2))
    AND s_store_name = 'ese'
) s5,
(
  SELECT COUNT(*) AS h11_to_11_30
  FROM store_sales
  JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk
  JOIN time_dim ON ss_sold_time_sk = t_time_sk
  JOIN store ON ss_store_sk = s_store_sk
  WHERE t_hour = 11 AND t_minute < 30
    AND ((hd_dep_count = 4 AND hd_vehicle_count <= 4 + 2)
      OR (hd_dep_count = 2 AND hd_vehicle_count <= 2 + 2)
      OR (hd_dep_count = 0 AND hd_vehicle_count <= 0 + 2))
    AND s_store_name = 'ese'
) s6,
(
  SELECT COUNT(*) AS h11_30_to_12
  FROM store_sales
  JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk
  JOIN time_dim ON ss_sold_time_sk = t_time_sk
  JOIN store ON ss_store_sk = s_store_sk
  WHERE t_hour = 11 AND t_minute >= 30
    AND ((hd_dep_count = 4 AND hd_vehicle_count <= 4 + 2)
      OR (hd_dep_count = 2 AND hd_vehicle_count <= 2 + 2)
      OR (hd_dep_count = 0 AND hd_vehicle_count <= 0 + 2))
    AND s_store_name = 'ese'
) s7,
(
  SELECT COUNT(*) AS h12_to_12_30
  FROM store_sales
  JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk
  JOIN time_dim ON ss_sold_time_sk = t_time_sk
  JOIN store ON ss_store_sk = s_store_sk
  WHERE t_hour = 12 AND t_minute < 30
    AND ((hd_dep_count = 4 AND hd_vehicle_count <= 4 + 2)
      OR (hd_dep_count = 2 AND hd_vehicle_count <= 2 + 2)
      OR (hd_dep_count = 0 AND hd_vehicle_count <= 0 + 2))
    AND s_store_name = 'ese'
) s8
LIMIT 100
