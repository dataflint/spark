package io.dataflint.example

import main.scala.io.dataflint.example.LargeBroadcastExample.{smallDfSize, spark}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object SetJobDescriptionAndUDFName extends App {
  val spark = SparkSession
    .builder()
    .appName("SetJobDescriptionAndUDFName")
    .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin")
    .config("spark.ui.port", "10000")
    .config("spark.dataflint.telemetry.enabled", value = false)
    .config("spark.sql.maxMetadataStringLength", "10000")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val df = spark.range(1L, 1000).toDF("id")
  val plusOne = udf((x: Int) => x + 1)

  df.filter(plusOne($"id") =!= 5).count()

  spark.sparkContext.setJobDescription("Range 1 to 1000 and then filter plus one not equal to 5")
  df.filter(plusOne($"id") =!= 5).count()

  val plusOneNamed = udf((x: Int) => x + 1).withName("plusOne")
  spark.sparkContext.setJobDescription("Range 1 to 1000 and then filter plus one not equal to 5, named")
  df.filter(plusOneNamed($"id") =!= 5).count()

  scala.io.StdIn.readLine()
  spark.stop()
}
