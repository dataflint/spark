package io.dataflint.example

import org.apache.spark.sql.SparkSession

object SchedulingSmallTasksSkipAlerts extends App {
  val spark = SparkSession
    .builder()
    .appName("SchedulingSmallTasks")
    .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin")
    .config("spark.dataflint.telemetry.enabled", false)
    .config("spark.ui.port", "10000")
    .config("spark.dataflint.telemetry.enabled", value = false)
    .config("spark.sql.maxMetadataStringLength", "10000")
    .config("spark.dataflint.alert.disabled", "smallTasks,idleCoresTooHigh")
    .master("local[*]")
    .getOrCreate()

  val numbers = spark.range(0, 10000).repartition(10000).count()

  println(s"count numbers to 10000: $numbers")

  scala.io.StdIn.readLine()
  spark.stop()
}
