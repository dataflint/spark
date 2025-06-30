package io.dataflint.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ExplodeExample extends App {
  val spark = SparkSession
    .builder()
    .appName("ExplodeExample")
    .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin")
    .config("spark.ui.port", "10000")
    .config("spark.dataflint.telemetry.enabled", value = false)
    .config("spark.eventLog.enabled", "true")
    .config("spark.sql.maxMetadataStringLength", "10000")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // Create a DataFrame with array columns
  val arrayDF = Seq(
    (1, Array("apple", "banana", "orange")),
    (2, Array("red", "green", "blue")),
    (3, Array("dog", "cat")),
    (4, Array.empty[String])
  ).toDF("id", "items")

  // Show original DataFrame
  println("Original DataFrame:")
  arrayDF.show(false)

  // Example 1: Basic explode
  println("Example 1: Basic explode (note: rows with empty arrays are dropped)")
  spark.sparkContext.setJobDescription("Basic Explode Operator")
  val explodedDF = arrayDF.select($"id", explode($"items").as("item"))
  explodedDF.show(false)

  // Example 2: Using explode_outer to keep rows with empty arrays
  println("Example 2: explode_outer to keep rows with empty arrays")
  spark.sparkContext.setJobDescription("Explode Outer Operator")
  val explodeOuterDF = arrayDF.select($"id", explode_outer($"items").as("item"))
  explodeOuterDF.show(false)

  // Example 3: explode with maps
  println("Example 3: explode with maps")
  val mapDF = Seq(
    (1, Map("a" -> 1, "b" -> 2, "c" -> 3)),
    (2, Map("x" -> 10, "y" -> 20)),
    (3, Map.empty[String, Int])
  ).toDF("id", "properties")

  spark.sparkContext.setJobDescription("Explode Map Operator")
  mapDF.show(false)

  val explodedMapDF = mapDF.select(
    $"id",
    explode($"properties").as(Seq("key", "value"))
  )
  explodedMapDF.show(false)

  // Example 4: posexplode to include position information
  println("Example 4: posexplode to include position information")
  spark.sparkContext.setJobDescription("Posexplode Operator")
  val posexplodeDF = arrayDF.select(
    $"id",
    posexplode($"items").as(Seq("position", "item"))
  )
  posexplodeDF.show(false)

  // Example 5: Using inline to explode array of structs
  println("Example 5: Using inline to explode array of structs")
  val structArrayDF = Seq(
    (1, Array((10, "a"), (20, "b"), (30, "c"))),
    (2, Array((40, "d"), (50, "e"))),
    (3, Array.empty[(Int, String)])
  ).toDF("id", "structs")

  spark.sparkContext.setJobDescription("Inline Operator")
  structArrayDF.show(false)

  val inlineDF = structArrayDF.select(
    $"id",
    inline_outer($"structs")
  )
  inlineDF.select($"id", $"_1".as("num"), $"_2".as("letter")).show(false)

  scala.io.StdIn.readLine()
  spark.stop()
}
