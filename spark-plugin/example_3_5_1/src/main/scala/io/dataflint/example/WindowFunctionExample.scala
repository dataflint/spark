package io.dataflint.example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object WindowFunctionExample extends App {
  val spark = SparkSession
    .builder()
    .appName("Window Function Example")
    .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin")
    .config("spark.ui.port", "10000")
    .config("spark.dataflint.telemetry.enabled", value = false)
    .config("spark.dataflint.instrument.spark.window.enabled", value = true)
    .config("spark.sql.maxMetadataStringLength", "10000")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val shakespeareText: DataFrame = spark.read
    .format("csv")
    .option("sep", ";")
    .option("inferSchema", true)
    .load("./test_data/will_play_text.csv")
    .toDF("line_id", "play_name", "speech_number", "line_number", "speaker", "text_entry")

  spark.sparkContext.setJobDescription("Compute lines per speaker per play")
  val linesPerSpeakerPerPlay = shakespeareText
    .groupBy("play_name", "speaker")
    .agg(count("*").alias("line_count"))

  spark.sparkContext.setJobDescription("Rank speakers by line count within each play")
  val speakerWindow = Window.partitionBy("play_name").orderBy(col("line_count").desc)

  val rankedSpeakers = linesPerSpeakerPerPlay
    .withColumn("rank", rank().over(speakerWindow))
    .withColumn("dense_rank", dense_rank().over(speakerWindow))
    .withColumn("row_number", row_number().over(speakerWindow))
    .withColumn("total_lines_in_play", sum("line_count").over(Window.partitionBy("play_name")))
    .withColumn("pct_of_play", round(col("line_count") / col("total_lines_in_play") * 100, 2))

  rankedSpeakers
    .filter(col("rank") <= 5)
    .orderBy("play_name", "rank")
    .show(50, truncate = false)

  spark.sparkContext.setJobDescription("Running average of speech numbers per speaker")
  val lineWindow = Window.partitionBy("speaker").orderBy("line_id")

  val runningStats = shakespeareText
    .withColumn("cumulative_lines", count("*").over(lineWindow))
    .withColumn("running_avg_speech", avg("speech_number").over(lineWindow))
    .withColumn("lead_line", lead("text_entry", 1).over(lineWindow))
    .withColumn("lag_line", lag("text_entry", 1).over(lineWindow))

  runningStats
    .filter(col("speaker") === "HAMLET")
    .select("line_id", "speaker", "text_entry", "cumulative_lines", "running_avg_speech", "lead_line", "lag_line")
    .show(20, truncate = false)

  scala.io.StdIn.readLine()
  spark.stop()
}
