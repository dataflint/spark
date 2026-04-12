-- TPC-DS Synthetic Data for CI Testing
-- Generates small-scale TPC-DS tables for verifying DataFlint plugin compatibility

-- 1. date_dim (730 rows - 2 years of dates)
CREATE TABLE date_dim USING parquet AS
SELECT
  CAST(id AS INT) AS d_date_sk,
  CONCAT('AAAAAAAAA', LPAD(CAST(id AS STRING), 7, '0')) AS d_date_id,
  DATE_ADD('2000-01-01', CAST(id AS INT)) AS d_date,
  CAST(FLOOR(id / 30) AS INT) AS d_month_seq,
  CAST(FLOOR(id / 7) AS INT) AS d_week_seq,
  CAST(FLOOR(id / 91) AS INT) AS d_quarter_seq,
  YEAR(DATE_ADD('2000-01-01', CAST(id AS INT))) AS d_year,
  DAYOFWEEK(DATE_ADD('2000-01-01', CAST(id AS INT))) AS d_dow,
  MONTH(DATE_ADD('2000-01-01', CAST(id AS INT))) AS d_moy,
  DAYOFMONTH(DATE_ADD('2000-01-01', CAST(id AS INT))) AS d_dom,
  QUARTER(DATE_ADD('2000-01-01', CAST(id AS INT))) AS d_qoy,
  YEAR(DATE_ADD('2000-01-01', CAST(id AS INT))) AS d_fy_year,
  CAST(FLOOR(id / 91) AS INT) AS d_fy_quarter_seq,
  CAST(FLOOR(id / 7) AS INT) AS d_fy_week_seq,
  ELT(DAYOFWEEK(DATE_ADD('2000-01-01', CAST(id AS INT))), 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday') AS d_day_name,
  CONCAT(CAST(YEAR(DATE_ADD('2000-01-01', CAST(id AS INT))) AS STRING), 'Q', CAST(QUARTER(DATE_ADD('2000-01-01', CAST(id AS INT))) AS STRING)) AS d_quarter_name,
  CASE WHEN id % 20 = 0 THEN 'Y' ELSE 'N' END AS d_holiday,
  CASE WHEN DAYOFWEEK(DATE_ADD('2000-01-01', CAST(id AS INT))) IN (1, 7) THEN 'Y' ELSE 'N' END AS d_weekend,
  CASE WHEN id % 21 = 0 THEN 'Y' ELSE 'N' END AS d_following_holiday,
  CAST(id - DAYOFMONTH(DATE_ADD('2000-01-01', CAST(id AS INT))) + 1 AS INT) AS d_first_dom,
  CAST(id - DAYOFMONTH(DATE_ADD('2000-01-01', CAST(id AS INT))) + 28 AS INT) AS d_last_dom,
  CAST(id - 365 AS INT) AS d_same_day_ly,
  CAST(id - 91 AS INT) AS d_same_day_lq,
  CASE WHEN id = 365 THEN 'Y' ELSE 'N' END AS d_current_day,
  CASE WHEN FLOOR(id / 7) = 52 THEN 'Y' ELSE 'N' END AS d_current_week,
  CASE WHEN MONTH(DATE_ADD('2000-01-01', CAST(id AS INT))) = 6 THEN 'Y' ELSE 'N' END AS d_current_month,
  CASE WHEN QUARTER(DATE_ADD('2000-01-01', CAST(id AS INT))) = 2 THEN 'Y' ELSE 'N' END AS d_current_quarter,
  CASE WHEN YEAR(DATE_ADD('2000-01-01', CAST(id AS INT))) = 2001 THEN 'Y' ELSE 'N' END AS d_current_year
FROM RANGE(0, 730) AS t(id);

-- 2. time_dim (200 rows)
CREATE TABLE time_dim USING parquet AS
SELECT
  CAST(id AS INT) AS t_time_sk,
  CONCAT('AAAAAAAAA', LPAD(CAST(id AS STRING), 7, '0')) AS t_time_id,
  CAST(id * 432 AS INT) AS t_time,
  CAST(FLOOR((id * 432) / 3600) AS INT) AS t_hour,
  CAST(FLOOR(((id * 432) % 3600) / 60) AS INT) AS t_minute,
  CAST((id * 432) % 60 AS INT) AS t_second,
  CASE WHEN FLOOR((id * 432) / 3600) < 12 THEN 'AM' ELSE 'PM' END AS t_am_pm,
  ELT(CAST(FLOOR(id / 50) AS INT) + 1, 'first', 'second', 'third', 'fourth') AS t_shift,
  ELT(CAST((id % 4) AS INT) + 1, 'night', 'morning', 'afternoon', 'evening') AS t_sub_shift,
  ELT(CAST((id % 3) AS INT) + 1, 'breakfast', 'lunch', 'dinner') AS t_meal_time
FROM RANGE(0, 200) AS t(id);

-- 3. item (200 rows)
CREATE TABLE item USING parquet AS
SELECT
  CAST(id AS INT) AS i_item_sk,
  CONCAT('AAAAAAAAA', LPAD(CAST(id AS STRING), 7, '0')) AS i_item_id,
  DATE_ADD('1997-01-01', CAST(id % 365 AS INT)) AS i_rec_start_date,
  DATE_ADD('2002-01-01', CAST(id % 365 AS INT)) AS i_rec_end_date,
  CONCAT('Item description ', CAST(id AS STRING)) AS i_item_desc,
  CAST(10.0 + (id % 500) * 0.5 AS DECIMAL(7,2)) AS i_current_price,
  CAST(5.0 + (id % 250) * 0.3 AS DECIMAL(7,2)) AS i_wholesale_cost,
  CAST((id % 20) + 1 AS INT) AS i_brand_id,
  CONCAT('Brand #', CAST((id % 20) + 1 AS STRING)) AS i_brand,
  CAST((id % 15) + 1 AS INT) AS i_class_id,
  ELT(CAST((id % 10) AS INT) + 1, 'shirts', 'pants', 'dresses', 'shoes', 'accessories', 'jewelry', 'electronics', 'books', 'music', 'sports') AS i_class,
  CAST((id % 10) + 1 AS INT) AS i_category_id,
  ELT(CAST((id % 10) AS INT) + 1, 'Women', 'Men', 'Children', 'Shoes', 'Music', 'Books', 'Electronics', 'Home', 'Sports', 'Jewelry') AS i_category,
  CAST((id % 50) + 1 AS INT) AS i_manufact_id,
  CONCAT('Manufact_', CAST((id % 50) + 1 AS STRING)) AS i_manufact,
  ELT(CAST((id % 6) AS INT) + 1, 'small', 'medium', 'large', 'extra large', 'petite', 'N/A') AS i_size,
  CONCAT('formulation_', CAST(id AS STRING)) AS i_formulation,
  ELT(CAST((id % 10) AS INT) + 1, 'red', 'blue', 'green', 'yellow', 'white', 'black', 'purple', 'orange', 'brown', 'pink') AS i_color,
  ELT(CAST((id % 5) AS INT) + 1, 'Each', 'Oz', 'Lb', 'Gram', 'Dozen') AS i_units,
  ELT(CAST((id % 4) AS INT) + 1, 'Unknown', 'Small', 'Medium', 'Large') AS i_container,
  CAST((id % 20) + 1 AS INT) AS i_manager_id,
  CONCAT('Product_', CAST(id AS STRING)) AS i_product_name
FROM RANGE(0, 200) AS t(id);

-- 4. customer (500 rows)
CREATE TABLE customer USING parquet AS
SELECT
  CAST(id AS INT) AS c_customer_sk,
  CONCAT('AAAAAAAAA', LPAD(CAST(id AS STRING), 7, '0')) AS c_customer_id,
  CAST((id % 200) AS INT) AS c_current_cdemo_sk,
  CAST((id % 100) AS INT) AS c_current_hdemo_sk,
  CAST((id % 500) AS INT) AS c_current_addr_sk,
  CAST((id % 730) AS INT) AS c_first_shipto_date_sk,
  CAST((id % 730) AS INT) AS c_first_sales_date_sk,
  ELT(CAST((id % 4) AS INT) + 1, 'Mr.', 'Mrs.', 'Ms.', 'Dr.') AS c_salutation,
  ELT(CAST((id % 10) AS INT) + 1, 'James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer', 'Michael', 'Linda', 'William', 'Elizabeth') AS c_first_name,
  ELT(CAST((id % 10) AS INT) + 1, 'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez') AS c_last_name,
  CASE WHEN id % 2 = 0 THEN 'Y' ELSE 'N' END AS c_preferred_cust_flag,
  CAST((id % 28) + 1 AS INT) AS c_birth_day,
  CAST((id % 12) + 1 AS INT) AS c_birth_month,
  CAST(1940 + (id % 60) AS INT) AS c_birth_year,
  ELT(CAST((id % 5) AS INT) + 1, 'UNITED STATES', 'CANADA', 'MEXICO', 'UNITED KINGDOM', 'GERMANY') AS c_birth_country,
  CAST(NULL AS STRING) AS c_login,
  CONCAT('customer', CAST(id AS STRING), '@example.com') AS c_email_address,
  CAST((id % 730) AS INT) AS c_last_review_date_sk
FROM RANGE(0, 500) AS t(id);

-- 5. customer_address (500 rows)
CREATE TABLE customer_address USING parquet AS
SELECT
  CAST(id AS INT) AS ca_address_sk,
  CONCAT('AAAAAAAAA', LPAD(CAST(id AS STRING), 7, '0')) AS ca_address_id,
  CAST((id % 999) + 1 AS STRING) AS ca_street_number,
  CONCAT('Street_', CAST(id AS STRING)) AS ca_street_name,
  ELT(CAST((id % 5) AS INT) + 1, 'Ave', 'Blvd', 'St', 'Dr', 'Rd') AS ca_street_type,
  CONCAT('Suite ', CAST(id % 100 AS STRING)) AS ca_suite_number,
  ELT(CAST((id % 10) AS INT) + 1, 'Midway', 'Fairview', 'Centerville', 'Georgetown', 'Five Points', 'Pleasant Hill', 'Oakland', 'Riverside', 'Union', 'Salem') AS ca_city,
  ELT(CAST((id % 10) AS INT) + 1, 'Williamson County', 'Daviess County', 'Walker County', 'Barrow County', 'Ziebach County', 'Luce County', 'Richland County', 'Oglethorpe County', 'Franklin Parish', 'Marion County') AS ca_county,
  ELT(CAST((id % 50) AS INT) + 1, 'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY') AS ca_state,
  LPAD(CAST(10000 + id % 90000 AS STRING), 5, '0') AS ca_zip,
  'United States' AS ca_country,
  CAST(-5.0 - (id % 4) AS DECIMAL(5,2)) AS ca_gmt_offset,
  ELT(CAST((id % 3) AS INT) + 1, 'single family', 'condo', 'apartment') AS ca_location_type
FROM RANGE(0, 500) AS t(id);

-- 6. customer_demographics (200 rows)
CREATE TABLE customer_demographics USING parquet AS
SELECT
  CAST(id AS INT) AS cd_demo_sk,
  ELT(CAST((id % 2) AS INT) + 1, 'M', 'F') AS cd_gender,
  ELT(CAST((id % 4) AS INT) + 1, 'M', 'S', 'D', 'W') AS cd_marital_status,
  ELT(CAST((id % 7) AS INT) + 1, 'Primary', 'Secondary', '2 yr Degree', '4 yr Degree', 'Advanced Degree', 'College', 'Unknown') AS cd_education_status,
  CAST(500 + (id % 10) * 500 AS INT) AS cd_purchase_estimate,
  ELT(CAST((id % 4) AS INT) + 1, 'Good', 'High Risk', 'Low Risk', 'Unknown') AS cd_credit_rating,
  CAST(id % 7 AS INT) AS cd_dep_count,
  CAST(id % 5 AS INT) AS cd_dep_employed_count,
  CAST(id % 4 AS INT) AS cd_dep_college_count
FROM RANGE(0, 200) AS t(id);

-- 7. household_demographics (100 rows)
CREATE TABLE household_demographics USING parquet AS
SELECT
  CAST(id AS INT) AS hd_demo_sk,
  CAST((id % 20) + 1 AS INT) AS hd_income_band_sk,
  ELT(CAST((id % 5) AS INT) + 1, '0-500', '501-1000', '1001-5000', '5001-10000', '>10000') AS hd_buy_potential,
  CAST(id % 7 AS INT) AS hd_dep_count,
  CAST(id % 5 AS INT) AS hd_vehicle_count
FROM RANGE(0, 100) AS t(id);

-- 8. store (20 rows)
CREATE TABLE store USING parquet AS
SELECT
  CAST(id AS INT) AS s_store_sk,
  CONCAT('AAAAAAAAA', LPAD(CAST(id AS STRING), 7, '0')) AS s_store_id,
  DATE_ADD('1997-01-01', CAST(id * 30 AS INT)) AS s_rec_start_date,
  DATE_ADD('2003-01-01', CAST(id * 30 AS INT)) AS s_rec_end_date,
  CAST(NULL AS INT) AS s_closed_date_sk,
  CONCAT('Store_', CAST(id AS STRING)) AS s_store_name,
  CAST(100 + id * 10 AS INT) AS s_number_employees,
  CAST(5000 + id * 500 AS INT) AS s_floor_space,
  ELT(CAST((id % 3) AS INT) + 1, '8AM-4PM', '8AM-12AM', '8AM-8PM') AS s_hours,
  CONCAT('Manager_', CAST(id AS STRING)) AS s_manager,
  CAST((id % 5) + 1 AS INT) AS s_market_id,
  ELT(CAST((id % 2) AS INT) + 1, 'Medium', 'Large') AS s_geography_class,
  CONCAT('Market desc ', CAST(id AS STRING)) AS s_market_desc,
  CONCAT('MarketMgr_', CAST(id AS STRING)) AS s_market_manager,
  CAST((id % 3) + 1 AS INT) AS s_division_id,
  CONCAT('Division_', CAST((id % 3) + 1 AS STRING)) AS s_division_name,
  CAST((id % 2) + 1 AS INT) AS s_company_id,
  CONCAT('Company_', CAST((id % 2) + 1 AS STRING)) AS s_company_name,
  CAST(100 + id AS STRING) AS s_street_number,
  CONCAT('Main St ', CAST(id AS STRING)) AS s_street_name,
  'Ave' AS s_street_type,
  CONCAT('Suite ', CAST(id AS STRING)) AS s_suite_number,
  ELT(CAST((id % 5) AS INT) + 1, 'Midway', 'Fairview', 'Centerville', 'Georgetown', 'Five Points') AS s_city,
  ELT(CAST((id % 5) AS INT) + 1, 'Williamson County', 'Daviess County', 'Walker County', 'Barrow County', 'Ziebach County') AS s_county,
  ELT(CAST((id % 5) AS INT) + 1, 'TN', 'KY', 'GA', 'AL', 'SD') AS s_state,
  LPAD(CAST(10000 + id * 1000 AS STRING), 5, '0') AS s_zip,
  'United States' AS s_country,
  CAST(-5.00 AS DECIMAL(5,2)) AS s_gmt_offset,
  CAST(0.05 + (id % 5) * 0.01 AS DECIMAL(5,2)) AS s_tax_percentage
FROM RANGE(0, 20) AS t(id);

-- 9. catalog_page (100 rows)
CREATE TABLE catalog_page USING parquet AS
SELECT
  CAST(id AS INT) AS cp_catalog_page_sk,
  CONCAT('AAAAAAAAA', LPAD(CAST(id AS STRING), 7, '0')) AS cp_catalog_page_id,
  CAST((id % 730) AS INT) AS cp_start_date_sk,
  CAST((id % 730) + 30 AS INT) AS cp_end_date_sk,
  ELT(CAST((id % 3) AS INT) + 1, 'DEPARTMENT1', 'DEPARTMENT2', 'DEPARTMENT3') AS cp_department,
  CAST((id / 10) + 1 AS INT) AS cp_catalog_number,
  CAST((id % 10) + 1 AS INT) AS cp_catalog_page_number,
  CONCAT('Catalog page description ', CAST(id AS STRING)) AS cp_description,
  ELT(CAST((id % 3) AS INT) + 1, 'bi-annual', 'quarterly', 'monthly') AS cp_type
FROM RANGE(0, 100) AS t(id);

-- 10. web_page (50 rows)
CREATE TABLE web_page USING parquet AS
SELECT
  CAST(id AS INT) AS wp_web_page_sk,
  CONCAT('AAAAAAAAA', LPAD(CAST(id AS STRING), 7, '0')) AS wp_web_page_id,
  DATE_ADD('1997-01-01', CAST(id * 30 AS INT)) AS wp_rec_start_date,
  DATE_ADD('2003-01-01', CAST(id * 30 AS INT)) AS wp_rec_end_date,
  CAST((id % 730) AS INT) AS wp_creation_date_sk,
  CAST((id % 730) AS INT) AS wp_access_date_sk,
  CASE WHEN id % 2 = 0 THEN 'Y' ELSE 'N' END AS wp_autogen_flag,
  CAST((id % 500) AS INT) AS wp_customer_sk,
  CONCAT('http://www.example.com/page', CAST(id AS STRING)) AS wp_url,
  ELT(CAST((id % 4) AS INT) + 1, 'welcome', 'ad', 'feedback', 'order') AS wp_type,
  CAST(1000 + id * 100 AS INT) AS wp_char_count,
  CAST(id % 20 AS INT) AS wp_link_count,
  CAST(id % 10 AS INT) AS wp_image_count,
  CAST(id % 5 AS INT) AS wp_max_ad_count
FROM RANGE(0, 50) AS t(id);

-- 11. web_site (10 rows)
CREATE TABLE web_site USING parquet AS
SELECT
  CAST(id AS INT) AS web_site_sk,
  CONCAT('AAAAAAAAA', LPAD(CAST(id AS STRING), 7, '0')) AS web_site_id,
  DATE_ADD('1997-01-01', CAST(id * 100 AS INT)) AS web_rec_start_date,
  DATE_ADD('2003-01-01', CAST(id * 100 AS INT)) AS web_rec_end_date,
  CONCAT('site_', CAST(id AS STRING)) AS web_name,
  CAST((id % 730) AS INT) AS web_open_date_sk,
  CAST(NULL AS INT) AS web_close_date_sk,
  ELT(CAST((id % 2) AS INT) + 1, 'Medium', 'Large') AS web_class,
  CONCAT('Manager_', CAST(id AS STRING)) AS web_manager,
  CAST((id % 3) + 1 AS INT) AS web_mkt_id,
  CONCAT('MktClass_', CAST(id AS STRING)) AS web_mkt_class,
  CONCAT('Market desc ', CAST(id AS STRING)) AS web_mkt_desc,
  CONCAT('MarketMgr_', CAST(id AS STRING)) AS web_market_manager,
  CAST((id % 2) + 1 AS INT) AS web_company_id,
  CONCAT('Company_', CAST((id % 2) + 1 AS STRING)) AS web_company_name,
  CAST(100 + id AS STRING) AS web_street_number,
  CONCAT('Web St ', CAST(id AS STRING)) AS web_street_name,
  'Blvd' AS web_street_type,
  CONCAT('Suite ', CAST(id AS STRING)) AS web_suite_number,
  ELT(CAST((id % 3) AS INT) + 1, 'Midway', 'Fairview', 'Centerville') AS web_city,
  'Williamson County' AS web_county,
  ELT(CAST((id % 3) AS INT) + 1, 'TN', 'KY', 'GA') AS web_state,
  '37110' AS web_zip,
  'United States' AS web_country,
  CAST(-5.00 AS DECIMAL(5,2)) AS web_gmt_offset,
  CAST(0.07 AS DECIMAL(5,2)) AS web_tax_percentage
FROM RANGE(0, 10) AS t(id);

-- 12. warehouse (10 rows)
CREATE TABLE warehouse USING parquet AS
SELECT
  CAST(id AS INT) AS w_warehouse_sk,
  CONCAT('AAAAAAAAA', LPAD(CAST(id AS STRING), 7, '0')) AS w_warehouse_id,
  CONCAT('Warehouse_', CAST(id AS STRING)) AS w_warehouse_name,
  CAST(50000 + id * 10000 AS INT) AS w_warehouse_sq_ft,
  CAST(100 + id AS STRING) AS w_street_number,
  CONCAT('Warehouse St ', CAST(id AS STRING)) AS w_street_name,
  'Ave' AS w_street_type,
  CONCAT('Suite ', CAST(id AS STRING)) AS w_suite_number,
  ELT(CAST((id % 3) AS INT) + 1, 'Midway', 'Fairview', 'Centerville') AS w_city,
  'Williamson County' AS w_county,
  ELT(CAST((id % 3) AS INT) + 1, 'TN', 'KY', 'GA') AS w_state,
  '37110' AS w_zip,
  'United States' AS w_country,
  CAST(-5.00 AS DECIMAL(5,2)) AS w_gmt_offset
FROM RANGE(0, 10) AS t(id);

-- 13. promotion (50 rows)
CREATE TABLE promotion USING parquet AS
SELECT
  CAST(id AS INT) AS p_promo_sk,
  CONCAT('AAAAAAAAA', LPAD(CAST(id AS STRING), 7, '0')) AS p_promo_id,
  CAST((id % 730) AS INT) AS p_start_date_sk,
  CAST((id % 730) + 14 AS INT) AS p_end_date_sk,
  CAST((id % 200) AS INT) AS p_item_sk,
  CAST(100.0 + id * 10 AS DECIMAL(7,2)) AS p_cost,
  CAST(id % 100 AS INT) AS p_response_target,
  CONCAT('Promo_', CAST(id AS STRING)) AS p_promo_name,
  CASE WHEN id % 2 = 0 THEN 'Y' ELSE 'N' END AS p_channel_dmail,
  CASE WHEN id % 3 = 0 THEN 'Y' ELSE 'N' END AS p_channel_email,
  CASE WHEN id % 4 = 0 THEN 'Y' ELSE 'N' END AS p_channel_catalog,
  CASE WHEN id % 5 = 0 THEN 'Y' ELSE 'N' END AS p_channel_tv,
  CASE WHEN id % 6 = 0 THEN 'Y' ELSE 'N' END AS p_channel_radio,
  CASE WHEN id % 7 = 0 THEN 'Y' ELSE 'N' END AS p_channel_press,
  CASE WHEN id % 8 = 0 THEN 'Y' ELSE 'N' END AS p_channel_event,
  CASE WHEN id % 9 = 0 THEN 'Y' ELSE 'N' END AS p_channel_demo,
  CONCAT('Channel details ', CAST(id AS STRING)) AS p_channel_details,
  ELT(CAST((id % 3) AS INT) + 1, 'Unknown', 'Promotion', 'Clearance') AS p_purpose,
  CASE WHEN id % 2 = 0 THEN 'Y' ELSE 'N' END AS p_discount_active
FROM RANGE(0, 50) AS t(id);

-- 14. income_band (20 rows)
CREATE TABLE income_band USING parquet AS
SELECT
  CAST(id + 1 AS INT) AS ib_income_band_sk,
  CAST(id * 10000 AS INT) AS ib_lower_bound,
  CAST((id + 1) * 10000 - 1 AS INT) AS ib_upper_bound
FROM RANGE(0, 20) AS t(id);

-- 15. ship_mode (20 rows)
CREATE TABLE ship_mode USING parquet AS
SELECT
  CAST(id AS INT) AS sm_ship_mode_sk,
  CONCAT('AAAAAAAAA', LPAD(CAST(id AS STRING), 7, '0')) AS sm_ship_mode_id,
  ELT(CAST((id % 5) AS INT) + 1, 'REGULAR', 'EXPRESS', 'OVERNIGHT', 'TWO DAY', 'LIBRARY') AS sm_type,
  ELT(CAST((id % 4) AS INT) + 1, 'AIR', 'SURFACE', 'SEA', 'RAIL') AS sm_code,
  ELT(CAST((id % 5) AS INT) + 1, 'UPS', 'FEDEX', 'DHL', 'USPS', 'AIRBORNE') AS sm_carrier,
  CONCAT('Contract_', CAST(id AS STRING)) AS sm_contract
FROM RANGE(0, 20) AS t(id);

-- 16. reason (35 rows)
CREATE TABLE reason USING parquet AS
SELECT
  CAST(id AS INT) AS r_reason_sk,
  CONCAT('AAAAAAAAA', LPAD(CAST(id AS STRING), 7, '0')) AS r_reason_id,
  ELT(CAST((id % 10) AS INT) + 1, 'reason 1', 'reason 2', 'reason 3', 'Did not like the color', 'Not the product that was ordered', 'Parts missing', 'Does not work with a product that I have', 'Gift exchange', 'Did not get it on time', 'Found a better price in a store') AS r_reason_desc
FROM RANGE(0, 35) AS t(id);

-- 17. call_center (10 rows)
CREATE TABLE call_center USING parquet AS
SELECT
  CAST(id AS INT) AS cc_call_center_sk,
  CONCAT('AAAAAAAAA', LPAD(CAST(id AS STRING), 7, '0')) AS cc_call_center_id,
  DATE_ADD('1998-01-01', CAST(id * 100 AS INT)) AS cc_rec_start_date,
  DATE_ADD('2003-01-01', CAST(id * 100 AS INT)) AS cc_rec_end_date,
  CAST(NULL AS INT) AS cc_closed_date_sk,
  CAST((id % 730) AS INT) AS cc_open_date_sk,
  CONCAT('Center_', CAST(id AS STRING)) AS cc_name,
  ELT(CAST((id % 3) AS INT) + 1, 'small', 'medium', 'large') AS cc_class,
  CAST(50 + id * 20 AS INT) AS cc_employees,
  CAST(1000 + id * 500 AS INT) AS cc_sq_ft,
  ELT(CAST((id % 3) AS INT) + 1, '8AM-4PM', '8AM-8PM', '8AM-12AM') AS cc_hours,
  CONCAT('Manager_', CAST(id AS STRING)) AS cc_manager,
  CAST((id % 3) + 1 AS INT) AS cc_mkt_id,
  CONCAT('MktClass_', CAST(id AS STRING)) AS cc_mkt_class,
  CONCAT('Market desc ', CAST(id AS STRING)) AS cc_mkt_desc,
  CONCAT('MarketMgr_', CAST(id AS STRING)) AS cc_market_manager,
  CAST((id % 3) + 1 AS INT) AS cc_division,
  CONCAT('Division_', CAST((id % 3) + 1 AS STRING)) AS cc_division_name,
  CAST((id % 2) + 1 AS INT) AS cc_company,
  CONCAT('Company_', CAST((id % 2) + 1 AS STRING)) AS cc_company_name,
  CAST(100 + id AS STRING) AS cc_street_number,
  CONCAT('CC St ', CAST(id AS STRING)) AS cc_street_name,
  'Ave' AS cc_street_type,
  CONCAT('Suite ', CAST(id AS STRING)) AS cc_suite_number,
  ELT(CAST((id % 3) AS INT) + 1, 'Midway', 'Fairview', 'Centerville') AS cc_city,
  'Williamson County' AS cc_county,
  ELT(CAST((id % 3) AS INT) + 1, 'TN', 'KY', 'GA') AS cc_state,
  '37110' AS cc_zip,
  'United States' AS cc_country,
  CAST(-5.00 AS DECIMAL(5,2)) AS cc_gmt_offset,
  CAST(0.07 AS DECIMAL(5,2)) AS cc_tax_percentage
FROM RANGE(0, 10) AS t(id);

-- ============================================================
-- FACT TABLES
-- ============================================================

-- 18. store_sales (2000 rows)
CREATE TABLE store_sales USING parquet AS
SELECT
  CAST((id % 730) AS INT) AS ss_sold_date_sk,
  CAST((id % 200) AS INT) AS ss_sold_time_sk,
  CAST((id % 200) AS INT) AS ss_item_sk,
  CAST((id % 500) AS INT) AS ss_customer_sk,
  CAST((id % 200) AS INT) AS ss_cdemo_sk,
  CAST((id % 100) AS INT) AS ss_hdemo_sk,
  CAST((id % 500) AS INT) AS ss_addr_sk,
  CAST((id % 20) AS INT) AS ss_store_sk,
  CAST((id % 50) AS INT) AS ss_promo_sk,
  CAST(id AS BIGINT) AS ss_ticket_number,
  CAST((id % 20) + 1 AS INT) AS ss_quantity,
  CAST(5.0 + (id % 50) * 0.5 AS DECIMAL(7,2)) AS ss_wholesale_cost,
  CAST(10.0 + (id % 100) * 0.5 AS DECIMAL(7,2)) AS ss_list_price,
  CAST(8.0 + (id % 80) * 0.5 AS DECIMAL(7,2)) AS ss_sales_price,
  CAST((id % 20) * 0.5 AS DECIMAL(7,2)) AS ss_ext_discount_amt,
  CAST((id % 100) * 1.5 AS DECIMAL(7,2)) AS ss_ext_sales_price,
  CAST((id % 50) * 1.0 AS DECIMAL(7,2)) AS ss_ext_wholesale_cost,
  CAST((id % 100) * 2.0 AS DECIMAL(7,2)) AS ss_ext_list_price,
  CAST((id % 10) * 0.5 AS DECIMAL(7,2)) AS ss_ext_tax,
  CAST((id % 15) * 0.3 AS DECIMAL(7,2)) AS ss_coupon_amt,
  CAST((id % 100) * 1.2 AS DECIMAL(7,2)) AS ss_net_paid,
  CAST((id % 100) * 1.3 AS DECIMAL(7,2)) AS ss_net_paid_inc_tax,
  CAST((id % 50) * 0.5 - 10 AS DECIMAL(7,2)) AS ss_net_profit
FROM RANGE(0, 2000) AS t(id);

-- 19. store_returns (500 rows)
CREATE TABLE store_returns USING parquet AS
SELECT
  CAST((id % 730) AS INT) AS sr_returned_date_sk,
  CAST((id % 200) AS INT) AS sr_return_time_sk,
  CAST((id % 200) AS INT) AS sr_item_sk,
  CAST((id % 500) AS INT) AS sr_customer_sk,
  CAST((id % 200) AS INT) AS sr_cdemo_sk,
  CAST((id % 100) AS INT) AS sr_hdemo_sk,
  CAST((id % 500) AS INT) AS sr_addr_sk,
  CAST((id % 20) AS INT) AS sr_store_sk,
  CAST((id % 35) AS INT) AS sr_reason_sk,
  CAST(id AS BIGINT) AS sr_ticket_number,
  CAST((id % 10) + 1 AS INT) AS sr_return_quantity,
  CAST(10.0 + (id % 50) * 1.0 AS DECIMAL(7,2)) AS sr_return_amt,
  CAST((id % 10) * 0.5 AS DECIMAL(7,2)) AS sr_return_tax,
  CAST(10.0 + (id % 50) * 1.0 + (id % 10) * 0.5 AS DECIMAL(7,2)) AS sr_return_amt_inc_tax,
  CAST((id % 10) * 0.3 AS DECIMAL(7,2)) AS sr_fee,
  CAST((id % 20) * 0.5 AS DECIMAL(7,2)) AS sr_return_ship_cost,
  CAST(5.0 + (id % 30) * 0.5 AS DECIMAL(7,2)) AS sr_refunded_cash,
  CAST((id % 5) * 0.5 AS DECIMAL(7,2)) AS sr_reversed_charge,
  CAST((id % 10) * 1.0 AS DECIMAL(7,2)) AS sr_store_credit,
  CAST((id % 20) * 0.5 AS DECIMAL(7,2)) AS sr_net_loss
FROM RANGE(0, 500) AS t(id);

-- 20. catalog_sales (2000 rows)
CREATE TABLE catalog_sales USING parquet AS
SELECT
  CAST((id % 730) AS INT) AS cs_sold_date_sk,
  CAST((id % 200) AS INT) AS cs_sold_time_sk,
  CAST(((id + 10) % 730) AS INT) AS cs_ship_date_sk,
  CAST((id % 500) AS INT) AS cs_bill_customer_sk,
  CAST((id % 200) AS INT) AS cs_bill_cdemo_sk,
  CAST((id % 100) AS INT) AS cs_bill_hdemo_sk,
  CAST((id % 500) AS INT) AS cs_bill_addr_sk,
  CAST(((id + 100) % 500) AS INT) AS cs_ship_customer_sk,
  CAST(((id + 50) % 200) AS INT) AS cs_ship_cdemo_sk,
  CAST(((id + 25) % 100) AS INT) AS cs_ship_hdemo_sk,
  CAST(((id + 100) % 500) AS INT) AS cs_ship_addr_sk,
  CAST((id % 10) AS INT) AS cs_call_center_sk,
  CAST((id % 100) AS INT) AS cs_catalog_page_sk,
  CAST((id % 20) AS INT) AS cs_ship_mode_sk,
  CAST((id % 10) AS INT) AS cs_warehouse_sk,
  CAST((id % 200) AS INT) AS cs_item_sk,
  CAST((id % 50) AS INT) AS cs_promo_sk,
  CAST(id AS BIGINT) AS cs_order_number,
  CAST((id % 20) + 1 AS INT) AS cs_quantity,
  CAST(5.0 + (id % 50) * 0.5 AS DECIMAL(7,2)) AS cs_wholesale_cost,
  CAST(10.0 + (id % 100) * 0.5 AS DECIMAL(7,2)) AS cs_list_price,
  CAST(8.0 + (id % 80) * 0.5 AS DECIMAL(7,2)) AS cs_sales_price,
  CAST((id % 20) * 0.5 AS DECIMAL(7,2)) AS cs_ext_discount_amt,
  CAST((id % 100) * 1.5 AS DECIMAL(7,2)) AS cs_ext_sales_price,
  CAST((id % 50) * 1.0 AS DECIMAL(7,2)) AS cs_ext_wholesale_cost,
  CAST((id % 100) * 2.0 AS DECIMAL(7,2)) AS cs_ext_list_price,
  CAST((id % 30) * 0.5 AS DECIMAL(7,2)) AS cs_ext_ship_cost,
  CAST((id % 10) * 0.5 AS DECIMAL(7,2)) AS cs_ext_tax,
  CAST((id % 15) * 0.3 AS DECIMAL(7,2)) AS cs_coupon_amt,
  CAST((id % 100) * 1.2 AS DECIMAL(7,2)) AS cs_net_paid,
  CAST((id % 100) * 1.3 AS DECIMAL(7,2)) AS cs_net_paid_inc_tax,
  CAST((id % 100) * 1.5 AS DECIMAL(7,2)) AS cs_net_paid_inc_ship,
  CAST((id % 100) * 1.6 AS DECIMAL(7,2)) AS cs_net_paid_inc_ship_tax,
  CAST((id % 50) * 0.5 - 10 AS DECIMAL(7,2)) AS cs_net_profit
FROM RANGE(0, 2000) AS t(id);

-- 21. catalog_returns (500 rows)
CREATE TABLE catalog_returns USING parquet AS
SELECT
  CAST((id % 730) AS INT) AS cr_returned_date_sk,
  CAST((id % 200) AS INT) AS cr_return_time_sk,
  CAST((id % 200) AS INT) AS cr_item_sk,
  CAST((id % 500) AS INT) AS cr_refunded_customer_sk,
  CAST((id % 200) AS INT) AS cr_refunded_cdemo_sk,
  CAST((id % 100) AS INT) AS cr_refunded_hdemo_sk,
  CAST((id % 500) AS INT) AS cr_refunded_addr_sk,
  CAST(((id + 100) % 500) AS INT) AS cr_returning_customer_sk,
  CAST(((id + 50) % 200) AS INT) AS cr_returning_cdemo_sk,
  CAST(((id + 25) % 100) AS INT) AS cr_returning_hdemo_sk,
  CAST(((id + 100) % 500) AS INT) AS cr_returning_addr_sk,
  CAST((id % 10) AS INT) AS cr_call_center_sk,
  CAST((id % 100) AS INT) AS cr_catalog_page_sk,
  CAST((id % 20) AS INT) AS cr_ship_mode_sk,
  CAST((id % 10) AS INT) AS cr_warehouse_sk,
  CAST((id % 35) AS INT) AS cr_reason_sk,
  CAST(id AS BIGINT) AS cr_order_number,
  CAST((id % 10) + 1 AS INT) AS cr_return_quantity,
  CAST(10.0 + (id % 50) * 1.0 AS DECIMAL(7,2)) AS cr_return_amount,
  CAST((id % 10) * 0.5 AS DECIMAL(7,2)) AS cr_return_tax,
  CAST(10.0 + (id % 50) * 1.0 + (id % 10) * 0.5 AS DECIMAL(7,2)) AS cr_return_amt_inc_tax,
  CAST((id % 10) * 0.3 AS DECIMAL(7,2)) AS cr_fee,
  CAST((id % 20) * 0.5 AS DECIMAL(7,2)) AS cr_return_ship_cost,
  CAST(5.0 + (id % 30) * 0.5 AS DECIMAL(7,2)) AS cr_refunded_cash,
  CAST((id % 5) * 0.5 AS DECIMAL(7,2)) AS cr_reversed_charge,
  CAST((id % 10) * 1.0 AS DECIMAL(7,2)) AS cr_store_credit,
  CAST((id % 20) * 0.5 AS DECIMAL(7,2)) AS cr_net_loss
FROM RANGE(0, 500) AS t(id);

-- 22. web_sales (2000 rows)
CREATE TABLE web_sales USING parquet AS
SELECT
  CAST((id % 730) AS INT) AS ws_sold_date_sk,
  CAST((id % 200) AS INT) AS ws_sold_time_sk,
  CAST(((id + 7) % 730) AS INT) AS ws_ship_date_sk,
  CAST((id % 200) AS INT) AS ws_item_sk,
  CAST((id % 500) AS INT) AS ws_bill_customer_sk,
  CAST((id % 200) AS INT) AS ws_bill_cdemo_sk,
  CAST((id % 100) AS INT) AS ws_bill_hdemo_sk,
  CAST((id % 500) AS INT) AS ws_bill_addr_sk,
  CAST(((id + 150) % 500) AS INT) AS ws_ship_customer_sk,
  CAST(((id + 75) % 200) AS INT) AS ws_ship_cdemo_sk,
  CAST(((id + 30) % 100) AS INT) AS ws_ship_hdemo_sk,
  CAST(((id + 150) % 500) AS INT) AS ws_ship_addr_sk,
  CAST((id % 50) AS INT) AS ws_web_page_sk,
  CAST((id % 10) AS INT) AS ws_web_site_sk,
  CAST((id % 20) AS INT) AS ws_ship_mode_sk,
  CAST((id % 10) AS INT) AS ws_warehouse_sk,
  CAST((id % 50) AS INT) AS ws_promo_sk,
  CAST(id AS BIGINT) AS ws_order_number,
  CAST((id % 20) + 1 AS INT) AS ws_quantity,
  CAST(5.0 + (id % 50) * 0.5 AS DECIMAL(7,2)) AS ws_wholesale_cost,
  CAST(10.0 + (id % 100) * 0.5 AS DECIMAL(7,2)) AS ws_list_price,
  CAST(8.0 + (id % 80) * 0.5 AS DECIMAL(7,2)) AS ws_sales_price,
  CAST((id % 20) * 0.5 AS DECIMAL(7,2)) AS ws_ext_discount_amt,
  CAST((id % 100) * 1.5 AS DECIMAL(7,2)) AS ws_ext_sales_price,
  CAST((id % 50) * 1.0 AS DECIMAL(7,2)) AS ws_ext_wholesale_cost,
  CAST((id % 100) * 2.0 AS DECIMAL(7,2)) AS ws_ext_list_price,
  CAST((id % 30) * 0.5 AS DECIMAL(7,2)) AS ws_ext_ship_cost,
  CAST((id % 10) * 0.5 AS DECIMAL(7,2)) AS ws_ext_tax,
  CAST((id % 15) * 0.3 AS DECIMAL(7,2)) AS ws_coupon_amt,
  CAST((id % 100) * 1.2 AS DECIMAL(7,2)) AS ws_net_paid,
  CAST((id % 100) * 1.3 AS DECIMAL(7,2)) AS ws_net_paid_inc_tax,
  CAST((id % 100) * 1.5 AS DECIMAL(7,2)) AS ws_net_paid_inc_ship,
  CAST((id % 100) * 1.6 AS DECIMAL(7,2)) AS ws_net_paid_inc_ship_tax,
  CAST((id % 50) * 0.5 - 10 AS DECIMAL(7,2)) AS ws_net_profit
FROM RANGE(0, 2000) AS t(id);

-- 23. web_returns (500 rows)
CREATE TABLE web_returns USING parquet AS
SELECT
  CAST((id % 730) AS INT) AS wr_returned_date_sk,
  CAST((id % 200) AS INT) AS wr_return_time_sk,
  CAST((id % 200) AS INT) AS wr_item_sk,
  CAST((id % 500) AS INT) AS wr_refunded_customer_sk,
  CAST((id % 200) AS INT) AS wr_refunded_cdemo_sk,
  CAST((id % 100) AS INT) AS wr_refunded_hdemo_sk,
  CAST((id % 500) AS INT) AS wr_refunded_addr_sk,
  CAST(((id + 100) % 500) AS INT) AS wr_returning_customer_sk,
  CAST(((id + 50) % 200) AS INT) AS wr_returning_cdemo_sk,
  CAST(((id + 25) % 100) AS INT) AS wr_returning_hdemo_sk,
  CAST(((id + 100) % 500) AS INT) AS wr_returning_addr_sk,
  CAST((id % 50) AS INT) AS wr_web_page_sk,
  CAST((id % 35) AS INT) AS wr_reason_sk,
  CAST(id AS BIGINT) AS wr_order_number,
  CAST((id % 10) + 1 AS INT) AS wr_return_quantity,
  CAST(10.0 + (id % 50) * 1.0 AS DECIMAL(7,2)) AS wr_return_amt,
  CAST((id % 10) * 0.5 AS DECIMAL(7,2)) AS wr_return_tax,
  CAST(10.0 + (id % 50) * 1.0 + (id % 10) * 0.5 AS DECIMAL(7,2)) AS wr_return_amt_inc_tax,
  CAST((id % 10) * 0.3 AS DECIMAL(7,2)) AS wr_fee,
  CAST((id % 20) * 0.5 AS DECIMAL(7,2)) AS wr_return_ship_cost,
  CAST(5.0 + (id % 30) * 0.5 AS DECIMAL(7,2)) AS wr_refunded_cash,
  CAST((id % 5) * 0.5 AS DECIMAL(7,2)) AS wr_reversed_charge,
  CAST((id % 10) * 1.0 AS DECIMAL(7,2)) AS wr_account_credit,
  CAST((id % 20) * 0.5 AS DECIMAL(7,2)) AS wr_net_loss
FROM RANGE(0, 500) AS t(id);

-- 24. inventory (2000 rows)
CREATE TABLE inventory USING parquet AS
SELECT
  CAST((id % 730) AS INT) AS inv_date_sk,
  CAST((id % 200) AS INT) AS inv_item_sk,
  CAST((id % 10) AS INT) AS inv_warehouse_sk,
  CAST(100 + (id % 900) AS INT) AS inv_quantity_on_hand
FROM RANGE(0, 2000) AS t(id);
