package io.dataflint.example

import org.apache.spark.sql.SparkSession

import java.sql.Timestamp

object DeltaLakeStreaming extends App {
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

  import spark.implicits._

  val numbers = (1 to 10).toList

  // Create a streaming DataFrame
  val streamingNumbers = spark.readStream
    .format("rate")
    .option("rowsPerSecond", "1")
    .load()
    .as[(Long, Timestamp)]
    .flatMap(_ => numbers)
    .toDF("number")

  // Filter numbers divisible by 2
  val filteredStream = streamingNumbers
    .mapPartitions(i => {
      Thread.sleep(10000)
      i
    })(streamingNumbers.encoder)
    .filter($"number" % 2 === 0)

  // Output the result to the console
  val query = filteredStream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/tmp/delta/events/_checkpoints/")
    .start("/tmp/delta/eventsByCustomer")

  // Wait for the streaming query to finish
  query.awaitTermination()

  scala.io.StdIn.readLine()
  spark.stop()
}