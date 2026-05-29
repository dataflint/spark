package org.apache.spark.dataflint

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Verifies that TimedExec's codegen path (doProduce) correctly measures duration.
 *
 * Uses a conf-gated Thread.sleep injected into the generated code so the test can
 * assert a known minimum duration without relying on CPU-bound computation timing.
 *
 * spark.range() is used instead of toDF() because the optimizer folds
 * Filter(LocalTableScan) away entirely, eliminating FilterExec from the plan.
 * RangeExec cannot be folded, so FilterExec survives into the executed plan.
 */
class DataFlintCodegenExecSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll with SqlMetricTestHelper {

  private val SleepMs = 100L
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("DataFlintCodegenExecSpec")
      .config(DataflintSparkUICommonLoader.INSTRUMENT_SQL_NODES_ENABLED, "true")
      .config("spark.sql.codegen.wholeStage", "true")
      .config("spark.sql.adaptive.enabled", "false") // simpler plan inspection
      .config("spark.ui.enabled", "false")
      .config("spark.dataflint.test.codegenSleepMs", SleepMs.toString)
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

  private def findTimedFilter(df: DataFrame): TimedExec =
    executedPlan(df).collect {
      case t: TimedExec if t.child.getClass.getSimpleName == "FilterExec" => t
    }.head

  test("TimedExec wraps FilterExec inside WholeStageCodegenExec") {
    // spark.range produces RangeExec — optimizer can't fold the filter away
    val df = spark.range(0, 100, 1, 1).toDF().filter("id > 50")
    df.collect()

    val plan = executedPlan(df)
    val timedFilters = plan.collect {
      case t: TimedExec if t.child.getClass.getSimpleName == "FilterExec" => t
    }
    withClue("Expected TimedExec(FilterExec) in plan:\n" + plan.treeString) {
      timedFilters should not be empty
    }

    val wsce = plan.collect { case w: WholeStageCodegenExec => w }
    withClue("Expected WholeStageCodegenExec in plan:\n" + plan.treeString) {
      wsce should not be empty
    }
  }

  test("codegen duration metric captures injected sleep (single partition)") {
    // 1 partition → sleep fires once → duration >= SleepMs
    val df = spark.range(0, 100, 1, 1).toDF().filter("id > 50")
    df.collect()

    val duration = findTimedFilter(df).metrics("duration").value
    withClue(s"duration=$duration ms should be >= $SleepMs ms (injected sleep)") {
      duration should be >= SleepMs
    }
  }

  test("codegen duration metric scales with partition count") {
    // 1 partition → sleep fires once
    val df1 = spark.range(0, 100, 1, 1).toDF().filter("id > 50")
    df1.collect()
    val dur1 = findTimedFilter(df1).metrics("duration").value

    // 4 partitions → sleep fires 4 times (metric is additive across partitions)
    val df4 = spark.range(0, 100, 1, 4).toDF().filter("id > 50")
    df4.collect()
    val dur4 = findTimedFilter(df4).metrics("duration").value

    withClue(s"dur1=$dur1 ms should be >= $SleepMs ms") {
      dur1 should be >= SleepMs
    }
    withClue(s"dur4=$dur4 ms should be >= ${4 * SleepMs} ms (4 partitions × $SleepMs ms sleep)") {
      dur4 should be >= 4 * SleepMs
    }
    withClue(s"dur4=$dur4 ms should be > dur1=$dur1 ms (more partitions = more sleep)") {
      dur4 should be > dur1
    }

    // Verify EVERY partition took at least SleepMs — not just the sum
    implicit val sparkImplicit: SparkSession = spark
    val stats = metricMinMax(findTimedFilter(df4).metrics("duration"))
    withClue(s"min partition duration=${stats.min} ms should be >= $SleepMs ms (sleep fires once per partition)") {
      stats.min should be >= SleepMs
    }
  }
}