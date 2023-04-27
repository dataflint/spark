package com.upgraydd.example

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.Paths

object Shakespeare {
  def fsPath(resource: String): String =
    Paths.get(this.getClass.getResource(resource).toURI).toString

  def df(spark: SparkSession): DataFrame = spark.read
    .format("csv")
    .option("sep", ";")
    .option("inferSchema", true)
    .load(fsPath("will_play_text.csv"))
    .toDF("line_id", "play_name", "speech_number", "line_number", "speaker", "text_entry")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Shakespeare")
      .config("spark.plugins", "com.upgraydd.spark.SparkUpgrayddPlugin")
      .master("local[*]")
      .getOrCreate()

    // SparkUpgreydd.upgrade(spark.sparkContext)

    df(spark)
    scala.io.StdIn.readLine()
    spark.stop()
  }
}
