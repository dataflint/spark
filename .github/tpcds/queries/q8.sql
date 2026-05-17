-- TPC-DS Query 8
-- Compute the net profit of stores located in 400 zip codes with the largest
-- total sales for items purchased in a given quarter.
SELECT
  s_store_name,
  SUM(ss_net_profit) AS net_profit
FROM store_sales
JOIN date_dim ON ss_sold_date_sk = d_date_sk
JOIN store ON ss_store_sk = s_store_sk
JOIN customer_address ON s_zip = ca_zip
WHERE d_qoy = 2
  AND d_year = 1998
  AND ca_address_sk IN (
    SELECT ca_address_sk
    FROM customer_address
    WHERE SUBSTR(ca_zip, 1, 5) IN (
      '24128','76232','65084','87816','83926','77556','20548','26231','43848','15126',
      '91137','61265','98294','25782','17920','18426','98235','40081','84093','28577',
      '55565','17183','54601','67897','22752','86284','18376','38607','45200','21756',
      '29741','96765','29322','83396','45586','70073','28360','26647','97590','48460',
      '92301','63267','63726','71190','68466','96515','18845','22744','13354','75691',
      '94168','21216','38827','63073','78224','53263','32432','47305','43520','37498',
      '44694','46626','43941','37024','57445','53628','28898','94621','32905','73730',
      '64605','39711','18578','68076','18106','31095','63187','62437','84369','96383',
      '14922','92105','10267','53155','79374','11506','36691','76963','88091','20709'
    )
  )
GROUP BY s_store_name
ORDER BY s_store_name
LIMIT 100
