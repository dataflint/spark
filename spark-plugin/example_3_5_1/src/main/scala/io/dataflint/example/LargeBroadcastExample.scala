package main.scala.io.dataflint.example

import io.dataflint.example.LargeFilterCondition.spark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object LargeBroadcastExample extends App {
  val spark = SparkSession
    .builder()
    .appName("LargeBroadcastExample")
    .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin")
    .config("spark.dataflint.telemetry.enabled", false)
    .config("spark.ui.port", "10000")
    .config("spark.dataflint.telemetry.enabled", value = false)
    .config("spark.sql.maxMetadataStringLength", "10000")
    .config("spark.driver.maxResultSize", "10g")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setJobDescription("Join with large broadcast")

  val smallDfSize = sys.env.get("SMALL_DF_SIZE").map(_.toLong).getOrElse((40 * 1000 * 1000).toLong)
  val largeDFSize = sys.env.get("LARGE_DF_SIZE").map(_.toLong).getOrElse((100 * 1000 * 1000).toLong)

  val smallDF = spark.range(1L, smallDfSize).toDF("id")
  val largeDF = spark.range(1L, largeDFSize).toDF("item_sk")

  val joinedDF = largeDF.join(broadcast(smallDF), largeDF("item_sk") === smallDF("id"))

  joinedDF.count()

  spark.sparkContext.setJobDescription("Join with shuffle")
  val joinedWithShuffleDF = largeDF.join(smallDF, largeDF("item_sk") === smallDF("id"))

  joinedWithShuffleDF.count()

  scala.io.StdIn.readLine()
  spark.stop()
}