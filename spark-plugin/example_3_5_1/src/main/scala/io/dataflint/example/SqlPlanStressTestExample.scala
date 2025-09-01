package io.dataflint.example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object SqlPlanStressTestExample extends App {

  val spark = SparkSession
    .builder()
    .appName("SQL Plan Stress Test Example")
    .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin")
    .config("spark.ui.port", "10000")
    .config("spark.dataflint.telemetry.enabled", value = false)
    .config("spark.sql.maxMetadataStringLength", "10000")
    .config("spark.eventLog.enabled", "true")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  println("Starting SQL Plan Stress Test with 100 iterations...")

  // Create a list to store all DataFrames for union
  val dataFrames = scala.collection.mutable.ListBuffer[DataFrame]()

  // Loop 100 iterations to create datasets with different ranges
  for (i <- 1 to 100) {
    println(s"Processing iteration $i...")
    
    // Create a small dataset with range (different range for each iteration)
    val rangeStart = i * 1000
    val rangeEnd = rangeStart + 100
    
    val df = spark.range(rangeStart, rangeEnd)
      .toDF("id")
      .withColumn("iteration", lit(i))
      .withColumn("value", col("id") * 2)
      .withColumn("category", when(col("id") % 2 === 0, "even").otherwise("odd"))
    
    // Apply select and filter operations
    val processedDf = df
      .select(
        col("id"),
        col("iteration"),
        col("value"),
        col("category")
      )
      .filter(col("id") % 10 =!= 0) // Filter out multiples of 10
      .filter(col("value") > rangeStart + 50) // Additional filter condition
    
    // Add to the list for union
    dataFrames += processedDf
  }

  println("Creating large union of all results...")

  // Create a large union of all results
  val unionedDf = dataFrames.reduce(_.union(_))

  println("Running count on the union...")

  // Run count on the union
  val totalCount = unionedDf.count()
  
  println(s"Total count after union: $totalCount")

  scala.io.StdIn.readLine()
  spark.stop()
}
