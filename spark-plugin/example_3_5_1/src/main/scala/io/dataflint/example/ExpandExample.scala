package io.dataflint.example

import org.apache.spark.sql.{SparkSession, functions => F}

object ExpandExample extends App {
  val spark = SparkSession
    .builder()
    .appName("Expand ETL")
    .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin")
    .config("spark.ui.port", "10000")
    .master("local[*]")
    .getOrCreate()

  val salesFilesLocation = sys.env.getOrElse("SALES_FILES_LOCATION", throw new Exception("SALES_FILES_LOCATION env var not set"))

  val df = spark.read.load(salesFilesLocation)

  // Query with multiple count distincts
  val dfCounts = df.select(
    F.countDistinct(F.col("ss_customer_sk")).alias("distinct_customers"),
    F.countDistinct(F.col("ss_item_sk")).alias("distinct_items"),
    F.countDistinct(F.col("ss_store_sk")).alias("distinct_stores"),
    F.countDistinct(F.col("ss_promo_sk")).alias("distinct_promotions")
  )

  dfCounts.show()

  scala.io.StdIn.readLine("job ended, press any key to continue..")

  spark.stop()
}

