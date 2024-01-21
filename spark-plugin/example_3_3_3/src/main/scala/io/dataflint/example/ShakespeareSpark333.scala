package io.dataflint.example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object ShakespeareSpark333 extends App {
  def df(spark: SparkSession): DataFrame = spark.read
    .format("csv")
    .option("sep", ";")
    .option("inferSchema", true)
    .load("./test_data/will_play_text.csv")
    .toDF("line_id", "play_name", "speech_number", "line_number", "speaker", "text_entry")
    .repartition(1000)

  val spark = SparkSession
    .builder()
    .appName("Shakespeare Statistics")
    .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin")
    .config("spark.dataflint.telemetry.enabled", false)
    .config("spark.ui.port", "10000")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val shakespeareText = df(spark)

  shakespeareText.printSchema()

  val count = shakespeareText.count()
  println(s"number of records : $count")

  val uniqueSpeakers = shakespeareText.select($"speaker").distinct().count()
  println(s"number of unique speakers : $uniqueSpeakers")

  val uniqueWords = shakespeareText.select(explode(split($"text_entry", " "))).distinct().count()

  println(s"number of unique words : $uniqueWords")

  scala.io.StdIn.readLine()
  spark.stop()
}
