package org.apache.spark.dataflint

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

/**
 * Verifies that TimedExec wraps all SQL node types and that the duration metric is populated.
 *
 * Each test triggers a specific physical operator, executes the query, then asserts that
 * TimedExec wraps the target node and that `duration` > 0.
 */
class DataFlintSqlNodesSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var tmpDir: String = _

  private val sleepInCodeGenMs = 2L

  override def beforeAll(): Unit = {
    tmpDir = Files.createTempDirectory("dataflint_test").toString
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("DataFlintSqlNodesSpec")
      .config(DataflintSparkUICommonLoader.INSTRUMENT_SQL_NODES_ENABLED, "true")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.ui.enabled", "false")
      .config("spark.dataflint.test.codegenSleepMs", sleepInCodeGenMs)
      .withExtensions(new DataFlintInstrumentationExtension)
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  private def executedPlan(df: DataFrame) = df.queryExecution.executedPlan match {
    case aqe: AdaptiveSparkPlanExec => aqe.finalPhysicalPlan
    case p                          => p
  }

  /** Find a TimedExec wrapping a child whose class simple name matches `childName`. */
  private def findTimed(df: DataFrame, childName: String): Option[TimedExec] =
    executedPlan(df).collect {
      case t: TimedExec if t.child.getClass.getSimpleName == childName => t
    }.headOption

  /** Assert TimedExec wraps `childName` and duration > 0. */
  private def assertWrappedWithDuration(df: DataFrame, childName: String): Unit = {
    df.collect() // trigger execution
    val plan = executedPlan(df)
    val timed = findTimed(df, childName)
    withClue(s"Expected TimedExec($childName) in plan:\n${plan.treeString}") {
      timed should not be empty
    }
    val duration = timed.get.metrics("duration").value
    withClue(s"TimedExec($childName) duration=$duration ms should be >= $sleepInCodeGenMs") {
      duration should be >= sleepInCodeGenMs
    }
  }

  // ---------------------------------------------------------------------------
  // Base data: spark.range produces RangeExec which survives optimizer folding
  // ---------------------------------------------------------------------------
  private def baseDF = spark.range(0, 1000, 1, 2).toDF("id")

  // ---------------------------------------------------------------------------
  // Unary / simple operators
  // ---------------------------------------------------------------------------

  test("ProjectExec") {
    assertWrappedWithDuration(baseDF.select(col("id") + 1 as "id" + sleepInCodeGenMs), "ProjectExec")
  }

  test("FilterExec") {
    assertWrappedWithDuration(baseDF.filter("id > 500"), "FilterExec")
  }

  test("SortExec") {
    // sort by desc to prevent optimizer from eliminating the sort (Range is already asc)
    assertWrappedWithDuration(baseDF.sort(col("id").desc), "SortExec")
  }

  test("ExpandExec") {
    assertWrappedWithDuration(baseDF.rollup("id").count(), "ExpandExec")
  }

  test("GenerateExec") {
    assertWrappedWithDuration(
      baseDF.select(explode(array(col("id"), col("id") + 1)) as "val"),
      "GenerateExec"
    )
  }

  // ---------------------------------------------------------------------------
  // Aggregates
  // ---------------------------------------------------------------------------

  test("HashAggregateExec") {
    assertWrappedWithDuration(baseDF.groupBy("id").count(), "HashAggregateExec")
  }

  test("SortAggregateExec") {
    spark.conf.set("spark.sql.execution.useObjectHashAggregateExec", "false")
    try {
      assertWrappedWithDuration(
        baseDF.groupBy("id").agg(collect_list("id") as "ids"),
        "SortAggregateExec"
      )
    } finally {
      spark.conf.set("spark.sql.execution.useObjectHashAggregateExec", "true")
    }
  }

  // ---------------------------------------------------------------------------
  // Joins
  // ---------------------------------------------------------------------------

  private def smallDF = spark.range(0, 10, 1, 1).toDF("sid")

  test("BroadcastHashJoinExec") {
    assertWrappedWithDuration(
      baseDF.join(smallDF, col("id") === col("sid")),
      "BroadcastHashJoinExec"
    )
  }

  test("SortMergeJoinExec") {
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    try {
      assertWrappedWithDuration(
        baseDF.join(smallDF, col("id") === col("sid")),
        "SortMergeJoinExec"
      )
    } finally {
      spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
    }
  }

  test("BroadcastNestedLoopJoinExec") {
    assertWrappedWithDuration(
      baseDF.join(smallDF, col("id") > col("sid")),
      "BroadcastNestedLoopJoinExec"
    )
  }

  test("CartesianProductExec") {
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    try {
      assertWrappedWithDuration(
        baseDF.crossJoin(smallDF),
        "CartesianProductExec"
      )
    } finally {
      spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
    }
  }

  // ---------------------------------------------------------------------------
  // Window
  // ---------------------------------------------------------------------------

  test("WindowGroupLimitExec") {
    val w = org.apache.spark.sql.expressions.Window.partitionBy(col("id") % 5).orderBy("id")
    val df = baseDF.withColumn("rn", row_number().over(w)).filter(col("rn") <= 3).drop("rn")
    // WindowGroupLimitExec is a Spark 3.4+ optimization; on older versions this becomes
    // WindowExec + FilterExec. Check for either.
    df.collect()
    val plan = executedPlan(df)
    val windowGroupLimitExec = findTimed(df, "WindowGroupLimitExec")
    val hasWindowGroupLimit = windowGroupLimitExec.isDefined
    val windowExec = findTimed(df, "WindowExec")
    val hasWindow = windowExec.isDefined
    withClue(s"Expected TimedExec(WindowGroupLimitExec) or TimedExec(WindowExec) in plan:\n${plan.treeString}") {
      (hasWindowGroupLimit || hasWindow) shouldBe true
    }
    val duration = windowGroupLimitExec.orElse(windowExec).get.metrics("duration").value
    withClue(s"TimedExec(WindowExec) duration=$duration ms should be >= $sleepInCodeGenMs") {
      duration should be >= sleepInCodeGenMs
    }
  }

  // ---------------------------------------------------------------------------
  // I/O nodes
  // ---------------------------------------------------------------------------

  test("DataWritingCommandExec") {
    val path = s"$tmpDir/write_test"
    baseDF.write.mode("overwrite").parquet(path)
    spark.read.parquet(path).count() should be > 0L
  }

  test("FileSourceScanExec - row") {
    val path = s"$tmpDir/scan_test_json"
    baseDF.write.mode("overwrite").json(path)
    assertWrappedWithDuration(spark.read.json(path), "FileSourceScanExec")
  }

  test("FileSourceScanExec - columnar") {
    val path = s"$tmpDir/scan_test_parquet"
    baseDF.write.mode("overwrite").parquet(path)
    assertWrappedWithDuration(spark.read.parquet(path), "FileSourceScanExec")
  }

  test("InMemoryTableScanExec") {
    val cached = baseDF.cache()
    cached.count() // materialize the cache
    // Second read hits InMemoryTableScanExec
    assertWrappedWithDuration(cached.select("id"), "InMemoryTableScanExec")
    cached.unpersist()
  }
}