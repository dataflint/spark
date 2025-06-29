package io.dataflint.example

import org.apache.spark.sql.SparkSession

import java.sql.Timestamp

object DeltaLakeLiquidClusteringExample extends App {
  val spark = SparkSession
    .builder()
    .appName("DeltaLakeLiquidClusteringExample")
    .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin")
    .config("spark.dataflint.telemetry.enabled", false)
    .config("spark.ui.port", "10000")
    .config("spark.sql.maxMetadataStringLength", "10000")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  import org.apache.spark.sql.functions._
  import io.delta.tables._

  // Create dummy data
  spark.sparkContext.setJobDescription("Creating dummy data for Delta table")
  val data = (1 to 100000).map { i =>
    val category = if (i % 5 == 0) "A" else if (i % 3 == 0) "B" else "C"
    (i, category, i % 100)
  }.toDF("id", "category", "value")

  // Write data to Delta table
  spark.sparkContext.setJobDescription("Writing data to Delta table")
  val path = "/tmp/zorder_table"
  data.write
    .format("delta")
    .mode("overwrite")
    .save(path)

  // Register as table if needed
  spark.sparkContext.setJobDescription("Registering Delta table in Spark SQL")
  spark.sql(s"DROP TABLE IF EXISTS zorder_table")
  spark.sql(s"CREATE TABLE zorder_table USING DELTA LOCATION '$path'")

  // === ✅ Native Delta Lake Z-Ordering ===
  spark.sparkContext.setJobDescription("Running Z-Ordering optimization on Delta table")
  spark.sql("""
  OPTIMIZE zorder_table
  ZORDER BY (category)
""")

  // Read without filter and count
  val dfAll = spark.read.format("delta").load(path)
  spark.sparkContext.setJobDescription("Counting without filter on Delta table")
  val countAll = dfAll.count()
  println(s"Total rows: $countAll")

  spark.sparkContext.setJobDescription("Counting with filter on Delta table")
  // Read with filter and count
  val dfFiltered = spark.read.format("delta").load(path)
    .filter($"category" === "A")
  val countFiltered = dfFiltered.count()
  println(s"Filtered rows (category = 'A'): $countFiltered")
  scala.io.StdIn.readLine()
  spark.stop()
}