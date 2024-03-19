package io.dataflint.example

import org.apache.spark.sql.SparkSession

object KafkaStreaming extends App {

  val spark = SparkSession
    .builder()
    .appName("Simple Streaming")
    .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin")
    .config("spark.dataflint.telemetry.enabled", false)
    .config("spark.ui.port", "10000")
    .config("spark.sql.maxMetadataStringLength", "10000")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .master("local[*]")
    .getOrCreate()

  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "testtopic")
    .option("startingOffsets", "earliest")
    .load()

  val dfSelected = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

  // Filter numbers divisible by 2
  val filteredStream = dfSelected
    .mapPartitions(i => {
      Thread.sleep(10000)
      i
    })(dfSelected.encoder)

  // Output the result to the console
  val query = filteredStream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/tmp/delta/kafka2delta/_checkpoints/")
    .start("/tmp/delta/kafka2delta")

  // Wait for the streaming query to finish
  query.awaitTermination()

  scala.io.StdIn.readLine()
  spark.stop()
}
