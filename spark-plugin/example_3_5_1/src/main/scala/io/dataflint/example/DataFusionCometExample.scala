package io.dataflint.example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object DataFusionCometExample extends App {
  def df(spark: SparkSession): DataFrame = spark.read
    .format("csv")
    .option("sep", ";")
    .option("inferSchema", true)
    .load("./test_data/will_play_text.csv")
    .toDF("line_id", "play_name", "speech_number", "line_number", "speaker", "text_entry")
    .repartition(1000)

  val spark = SparkSession
    .builder()
    .appName("DataFusionCometExample")
    .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin,org.apache.spark.CometPlugin")
    .config("spark.shuffle.manager", "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
    .config("spark.comet.explainFallback.enabled", "true")
    .config("spark.memory.offHeap.enabled", "true")
    .config("spark.memory.offHeap.size", "16g")
    .config("spark.ui.port", "10000")
    .config("spark.dataflint.telemetry.enabled", value = false)
    .config("spark.sql.maxMetadataStringLength", "10000")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val shakespeareText = df(spark)

  shakespeareText.printSchema()

  val count = shakespeareText.count()
  println(s"number of records : $count")

  val uniqueSpeakers = shakespeareText.select($"speaker").filter($"line_id".isNotNull).distinct().count()
  println(s"number of unique speakers : $uniqueSpeakers")

  val uniqueWords = shakespeareText.select(explode(split($"text_entry", " "))).distinct().count()

  println(s"number of unique words : $uniqueWords")


  spark.read.load("/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/store_sales").filter($"ss_quantity" > 1).count()

  scala.io.StdIn.readLine()
  spark.stop()
}
