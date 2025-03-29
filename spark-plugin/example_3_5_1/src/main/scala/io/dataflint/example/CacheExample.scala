package io.dataflint.example

import io.dataflint.example.SchedulingSmallTasks.spark
import org.apache.spark.sql.SparkSession

object CacheExample extends App {
  val spark = SparkSession
    .builder()
    .appName("JobGroupExample")
    .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin")
    .config("spark.ui.port", "10000")
    .config("spark.dataflint.telemetry.enabled", value = false)
    .config("spark.eventLog.enabled", "true")
    .config("spark.sql.maxMetadataStringLength", "10000")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val df = spark.range(0, 10).cache()
  val secondCache = df.select($"id" * 2).persist()
  secondCache.count()
  df.unpersist()
  secondCache.unpersist()

  val df2 = spark.range(0, 10000000L).repartition(100).cache()
  df2.count()
  df.unpersist()

  scala.io.StdIn.readLine()

  spark.stop()
}
