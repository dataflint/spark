package org.apache.spark.dataflint.jobgroup.tests

import org.apache.spark.dataflint.jobgroup.JobGroupExtractor
import org.apache.spark.sql.SparkSession

class JobGroupTests extends org.scalatest.funsuite.AnyFunSuiteLike {
  test("test job group extractor with 2 groups") {
    val spark = SparkSession
      .builder()
      .appName("JobGroupExample")
      .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin")
      .config("spark.ui.port", "10000")
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

    spark.sparkContext.clearJobGroup()

    // Set up and run the second query with a different group ID
    spark.sparkContext.setJobGroup("queryGroup2", "Group 2: Average Scores")
    val avgScores = spark.sql("SELECT name, AVG(score) as avg_score FROM student_scores GROUP BY name")
    avgScores.count()

    // Optionally, clear job group if needed
    spark.sparkContext.clearJobGroup()

    Thread.sleep(1000)

    val extractor = new JobGroupExtractor(spark.sparkContext.ui.get.store, spark.sharedState.statusStore)
    val queryGroup1Store = extractor.extract("queryGroup1")
    val queryGroup2Store = extractor.extract("queryGroup2")

    assert(queryGroup1Store._2.executionsList().length == 1)
    assert(queryGroup2Store._2.executionsList().length == 1)
    spark.stop()
  }

}
