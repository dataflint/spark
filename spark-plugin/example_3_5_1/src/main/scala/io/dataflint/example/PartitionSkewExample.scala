package io.dataflint.example

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object PartitionSkewExample extends App {
  val spark = SparkSession
    .builder()
    .appName("Partition Skew Example")
    .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin")
    .config("spark.dataflint.telemetry.enabled", false)
    .config("spark.ui.port", "10000")
    .config("spark.dataflint.telemetry.enabled", value = false)
    .config("spark.sql.maxMetadataStringLength", "10000")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val numbers = spark.range(1, 100).repartition(100)

  val count = numbers.mapPartitions(i => {
    i.map(i => {
      if (i == 50L) {
        Thread.sleep(10000)
      }
      i
    });
    })(numbers.encoder)
    .count()

  println(s"count numbers: $count")

  scala.io.StdIn.readLine()
  spark.stop()
}
