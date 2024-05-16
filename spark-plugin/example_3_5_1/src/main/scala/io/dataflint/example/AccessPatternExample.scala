package io.dataflint.example

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object AccessPatternExample extends App {
  val spark = SparkSession
    .builder()
    .appName("AccessPatternExample")
    .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin")
    .config("spark.dataflint.telemetry.enabled", false)
    .config("spark.ui.port", "10000")
    .config("spark.dataflint.telemetry.enabled", value = false)
    .config("spark.sql.maxMetadataStringLength", "10000")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val salesDF = spark.read.load(sys.env("SALES_FILES_LOCATION"))

  spark.sparkContext.setJobDescription("full scan of store_sales")
  salesDF.count()

  spark.sparkContext.setJobDescription("scan of store_sales, filter by partition")
  salesDF
    .where($"ss_sold_date_sk" > 2450858)
    .count()

  spark.sparkContext.setJobDescription("scan of store_sales, filter by field")
  salesDF
    .where($"ss_quantity" > 1)
    .count()

  spark.sparkContext.setJobDescription("scan of store_sales, filter by partition and field")
  salesDF
    .where($"ss_sold_date_sk" > 2450858)
    .where($"ss_quantity" > 1)
    .count()

  spark.sparkContext.setJobDescription("scan of store_sales, filter by field condition")
  salesDF
    .where($"ss_sold_date_sk" > 2450858)
    .where($"ss_quantity" * 2 > 2)
    .count()

  spark.sparkContext.setJobDescription("scan of store_sales, filter by partition and field and field condition")
  salesDF
    .where($"ss_sold_date_sk" > 2450858)
    .where($"ss_store_sk" > 0)
    .where($"ss_quantity" * 2 > 2)
    .count()

  spark.sparkContext.setJobDescription("scan store_sales by partition, select 3 fields: ss_cdemo_s, ss_net_paid, ss_net_profit")
  salesDF
    .where($"ss_sold_date_sk" > 2450858)
    .select($"ss_cdemo_sk", $"ss_net_paid", $"ss_net_profit")
    .show()

  spark.sparkContext.setJobDescription("scan store_sales by partition, select all fields")
  salesDF
    .where($"ss_sold_date_sk" > 2450858)
    .show()

  scala.io.StdIn.readLine()
  spark.stop()
}
