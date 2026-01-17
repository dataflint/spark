package io.dataflint.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object DistributedComputeExample extends App {
  val spark = SparkSession
    .builder()
    .appName("Distributed Compute Examples")
    .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin")
    .config("spark.ui.port", "10000")
    .config("spark.dataflint.telemetry.enabled", value = false)
    .config("spark.sql.maxMetadataStringLength", "10000")
    .master("local[*]")
    .getOrCreate()

  val salesFilesLocation = sys.env.getOrElse("SALES_FILES_LOCATION", 
    throw new RuntimeException("SALES_FILES_LOCATION environment variable not set"))
  
  val df = spark.read.load(salesFilesLocation)

  // GroupBy store and customer - default partitioning
  spark.sparkContext.setJobDescription("GroupBy store and customer - default partitioning")
  val groupedCount1 = df.groupBy("ss_store_sk", "ss_customer_sk").count()
  groupedCount1.collect()

  // GroupBy store and customer - with repartition
  spark.sparkContext.setJobDescription("GroupBy store and customer - with repartition")
  val groupedCount2 = df.repartition(col("ss_store_sk"), col("ss_customer_sk"))
    .groupBy("ss_store_sk", "ss_customer_sk")
    .count()
  groupedCount2.collect()

  // GroupBy store and customer - with coalesce(6)
  spark.sparkContext.setJobDescription("GroupBy store and customer - with coalesce(6)")
  val groupedCount3 = df.coalesce(6)
    .groupBy("ss_store_sk", "ss_customer_sk")
    .count()
  groupedCount3.collect()

  // GroupBy store and customer - with salting
  val numSalts = 8
  val dfWithSalt = df.withColumn("salt", floor(rand(42) * numSalts))

  spark.sparkContext.setJobDescription("GroupBy store and customer - with salting")
  val groupedCountFinal = dfWithSalt
    .groupBy("ss_store_sk", "ss_customer_sk", "salt")
    .count()
    .groupBy("ss_store_sk", "ss_customer_sk")
    .count()
  groupedCountFinal.collect()

  // GroupBy with join - count by store and customer, filter > 3 or > 5
  spark.sparkContext.setJobDescription("GroupBy with join - count by store and customer, filter > 3 or > 5")
  val storeCounts = df.groupBy("ss_store_sk")
    .agg(count("*").alias("store_count"))
    .filter(col("store_count") > 3)

  val customerCounts = df.groupBy("ss_customer_sk")
    .agg(count("*").alias("customer_count"))
    .filter(col("customer_count") > 5)

  val joinResult = df.select("ss_store_sk", "ss_customer_sk")
    .join(storeCounts, Seq("ss_store_sk"), "left")
    .join(customerCounts, Seq("ss_customer_sk"), "left")
    .filter(col("store_count").isNotNull || col("customer_count").isNotNull)
    .count()

  println(s"Join result count: $joinResult")

  // Window functions - count by store and customer, filter > 3
  spark.sparkContext.setJobDescription("Window functions - count by store and customer, filter > 3")
  val dfWithStoreCount = df.select("ss_store_sk", "ss_customer_sk")
    .withColumn("store_count", count("*").over(Window.partitionBy("ss_store_sk")))
    .withColumn("customer_count", count("*").over(Window.partitionBy("ss_customer_sk")))
    .filter((col("store_count") > 3) || (col("customer_count") > 5))
    .count()

  println(s"Window function result count: $dfWithStoreCount")

  // Multiple countDistinct aggregations
  spark.sparkContext.setJobDescription("Multiple countDistinct aggregations")
  val dfCounts = df.select(
    countDistinct(col("ss_customer_sk")).alias("distinct_customers"),
    countDistinct(col("ss_item_sk")).alias("distinct_items"),
    countDistinct(col("ss_store_sk")).alias("distinct_stores"),
    countDistinct(col("ss_promo_sk")).alias("distinct_promotions")
  )
  dfCounts.show()

  scala.io.StdIn.readLine("example ended, press any key to continue..")
  spark.stop()
}

