package io.dataflint.example

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.Paths

object Shakespeare351ExportedLocal extends App {
  def df(spark: SparkSession): DataFrame = spark.read
    .format("csv")
    .option("sep", ";")
    .option("inferSchema", true)
    .load("./test_data/will_play_text.csv")
    .toDF("line_id", "play_name", "speech_number", "line_number", "speaker", "text_entry")
    .repartition(1000)

  val spark = SparkSession
    .builder
    .appName("Shakespeare Statistics Exported")
    .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin")
    .config("spark.dataflint.telemetry.enabled", false)
    .config("spark.ui.port", "10000")
    .config("spark.dataflint.mode", "local")
    .config("spark.dataflint.token", "AKIAZEUOHHYMKVUKYYZB-1234")
    .config("spark.sql.maxMetadataStringLength", "10000")
    .config("spark.eventLog.enabled", "true")
    .master("local[*]")
    .getOrCreate()

  val shakespeareText = df(spark)

  val count = shakespeareText.count()
  println(s"number of records : $count")

  spark.stop()
}
