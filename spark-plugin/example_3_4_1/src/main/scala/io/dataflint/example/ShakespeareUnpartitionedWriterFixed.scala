package io.dataflint.example

import org.apache.spark.sql.{SaveMode, SparkSession}

import java.nio.file.Paths

object ShakespeareUnpartitionedWriterFixed extends App {
  val spark = SparkSession
    .builder()
    .appName("Shakespeare Unpartitioned Writer Fixed")
    .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin")
    .config("spark.dataflint.telemetry.enabled", false)
    .config("spark.ui.port", "10000")
    .config("spark.eventLog.enabled", true)
    .config("spark.sql.maxMetadataStringLength", "10000")
    .master("local[*]")
    .getOrCreate()

  val shakespeareDF = spark.read
    .format("csv")
    .option("sep", ";")
    .option("inferSchema", true)
    .load("./test_data/will_play_text.csv")
    .toDF("line_id", "play_name", "speech_number", "line_number", "speaker", "text_entry")
    .repartition(200)

  shakespeareDF
    .repartition(1)
    .mapPartitions(itr => {
      // simulate slow write like in S3
      Thread.sleep(200)
      itr
    })(shakespeareDF.encoder)
    .write.mode(SaveMode.Overwrite).parquet("/tmp/shakespear")

  spark.stop()
}
