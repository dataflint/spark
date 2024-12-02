package io.dataflint.example

import org.apache.spark.sql.SparkSession

object JobGroupExportedLocal extends App {
  val spark = SparkSession
    .builder()
    .appName("JobGroupExample")
    .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin")
    .config("spark.ui.port", "10000")
    .config("spark.dataflint.telemetry.enabled", value = false)
    .config("spark.eventLog.enabled", "true")
    .config("spark.dataflint.mode", "local")
    .config("spark.dataflint.token", "AKIAZEUOHHYMKVUKYYZB-1234")
    .config("spark.sql.maxMetadataStringLength", "10000")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val data = Seq(
    ("Alice", "Math", 85),
    ("Alice", "Physics", 95),
    ("Bob", "Math", 78),
    ("Bob", "Physics", 88),
    ("Charlie", "Math", 92),
    ("Charlie", "Physics", 80)
  ).toDF("name", "subject", "score")

  data.createOrReplaceTempView("student_scores")

  // Set up and run the first query with a specific group ID
  spark.sparkContext.setJobGroup("queryGroup1", "Group 1: Math Scores")
  val mathScores = spark.sql("SELECT name, score FROM student_scores WHERE subject = 'Math'")
  mathScores.count()

  // Set up and run the second query with a different group ID
  spark.sparkContext.setJobGroup("queryGroup2", "Group 2: Average Scores")
  val avgScores = spark.sql("SELECT name, AVG(score) as avg_score FROM student_scores GROUP BY name")
  avgScores.count()

  spark.stop()
}
