package main.scala.io.dataflint.example

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
    .config("spark.driver.memory", "8g")
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", "/opt/spark/conf/spark-events")
    .master("local[*]")
    .getOrCreate()
  
  val numberRows = sys.env.get("NUMBER_OF_ROWS").map(_.toInt).getOrElse(100000)

  val largeDF = spark.range(1, 5000000).toDF("item_sk")

  val smallDF = spark.range(1, numberRows).toDF("id")

  val joinedDF = largeDF.join(broadcast(smallDF), largeDF("item_sk") === smallDF("id"))

  joinedDF.show()

  scala.io.StdIn.readLine()
  spark.stop()
}