package io.dataflint.example

import org.apache.spark.sql.{DataFrame, SparkSession}

object LargeFilterCondition extends App {
  val spark = SparkSession
    .builder()
    .appName("Large Filter Condition")
    .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin")
    .config("spark.dataflint.telemetry.enabled", false)
    .config("spark.ui.port", "10000")
    .config("spark.dataflint.telemetry.enabled", value = false)
    .config("spark.sql.maxMetadataStringLength", "10000")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val numOfConditions = 100
  val sizeOfDF = 100000

  val filterConditions = Range(0, numOfConditions).map($"id".equalTo(_)).reduce(_ || _)

  spark.sparkContext.setJobDescription("Filter with long filter condition")

  val countAfterLongFilter = spark.range(0, sizeOfDF)
    .filter(filterConditions)
    .count()

  println(s"count after long filter condition: ${countAfterLongFilter}")

  spark.sparkContext.setJobDescription("Filter using join")

  val filterTable = spark.range(0, numOfConditions).toDF("id")

  val countAfterJoinFilter = spark.range(0, sizeOfDF)
    .join(filterTable, "id")
    .count()

  println(s"count after filter using join: ${countAfterJoinFilter}")

  scala.io.StdIn.readLine()
  spark.stop()
}
