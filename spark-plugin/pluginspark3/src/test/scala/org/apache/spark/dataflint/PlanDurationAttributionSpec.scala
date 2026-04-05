package org.apache.spark.dataflint

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PlanDurationAttributionSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private val SleepMs = 2L
  private var sparkInstrumented: SparkSession = _

  override def beforeAll(): Unit = {
    sparkInstrumented = SparkSession.builder()
      .master("local[1]")
      .appName("PlanDurationAttributionSpec-instrumented")
      .config(DataflintSparkUICommonLoader.INSTRUMENT_SQL_NODES_ENABLED, "true")
      .config("spark.sql.codegen.wholeStage", "true")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.dataflint.test.codegenSleepMs", SleepMs.toString)
      .withExtensions(new DataFlintInstrumentationExtension)
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    // whichever session is active, stop it
    val active = SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession)
    active.foreach(_.stop())
  }

  private def executedPlan(df: DataFrame): SparkPlan = df.queryExecution.executedPlan match {
    case aqe: AdaptiveSparkPlanExec => aqe.finalPhysicalPlan
    case p                          => p
  }

  private def allNodeDurations(
      result: Map[Int, Seq[PlanDurationAttribution.NodeDuration]]
  ): Seq[PlanDurationAttribution.NodeDuration] =
    result.toSeq.sortBy(_._1).flatMap(_._2)

  /** Stop the instrumented session and create a fresh native (no instrumentation) session. */
  private def createNativeSession(): SparkSession = {
    sparkInstrumented.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    SparkSession.builder()
      .master("local[1]")
      .appName("PlanDurationAttributionSpec-native")
      .config("spark.sql.codegen.wholeStage", "true")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
  }

  // ---------------------------------------------------------------------------
  // Test 1: basic smoke test
  // ---------------------------------------------------------------------------
  test("compute returns non-empty stages with valid NodeDuration entries") {
    val df = sparkInstrumented.range(0, 100, 1, 1).toDF().filter("id > 50")
    df.collect()

    val result = PlanDurationAttribution.compute(executedPlan(df))
    withClue("result should be non-empty") {
      result should not be empty
    }

    val all = allNodeDurations(result)
    withClue("all exclusiveMs should be >= 0") {
      all.foreach { nd =>
        nd.exclusiveMs should be >= 0L
      }
    }
    withClue("at least one node should have exclusiveMs > 0 (codegen sleep)") {
      all.exists(_.exclusiveMs > 0) shouldBe true
    }
  }

  // ---------------------------------------------------------------------------
  // Test 2: pipelined subtraction
  // ---------------------------------------------------------------------------
  test("pipelined subtraction: parent exclusive < parent inclusive duration") {
    val df = sparkInstrumented.range(0, 100, 1, 1).toDF().filter("id > 50").select(col("id") + 1)
    df.collect()

    val plan = executedPlan(df)
    val result = PlanDurationAttribution.compute(plan)
    val all = allNodeDurations(result)

    // Find TimedExec wrapping ProjectExec and FilterExec
    val projectND = all.find { nd =>
      nd.node.isInstanceOf[TimedExec] &&
        nd.node.asInstanceOf[TimedExec].child.getClass.getSimpleName == "ProjectExec"
    }
    val filterND = all.find { nd =>
      nd.node.isInstanceOf[TimedExec] &&
        nd.node.asInstanceOf[TimedExec].child.getClass.getSimpleName == "FilterExec"
    }

    withClue(s"Expected TimedExec(ProjectExec) in plan:\n${plan.treeString}") {
      projectND shouldBe defined
    }
    withClue(s"Expected TimedExec(FilterExec) in plan:\n${plan.treeString}") {
      filterND shouldBe defined
    }

    val projectInclusive = projectND.get.node.metrics("duration").value
    withClue(s"Project exclusive=${projectND.get.exclusiveMs} should be < inclusive=$projectInclusive (subtraction happened)") {
      projectND.get.exclusiveMs should be < projectInclusive
    }

    // Filter is leaf timed node (RangeExec below has no timing) — no subtraction
    val filterInclusive = filterND.get.node.metrics("duration").value
    withClue(s"Filter exclusive=${filterND.get.exclusiveMs} should equal inclusive=$filterInclusive (leaf timed node)") {
      filterND.get.exclusiveMs shouldBe filterInclusive
    }
  }

  // ---------------------------------------------------------------------------
  // Test 3: shuffle produces multiple stages
  // ---------------------------------------------------------------------------
  test("shuffle query produces multiple stages") {
    // A sort-merge join guarantees Exchange nodes in the middle of the plan
    val prevThreshold = sparkInstrumented.conf.get("spark.sql.autoBroadcastJoinThreshold")
    sparkInstrumented.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    try {
      val left  = sparkInstrumented.range(0, 50, 1, 2).toDF("id")
      val right = sparkInstrumented.range(0, 50, 1, 2).toDF("id")
      val df = left.join(right, "id")
      df.collect()

      val plan = executedPlan(df)
      val result = PlanDurationAttribution.compute(plan)
      withClue(s"Expected >= 2 stages for join query, got ${result.size}\nPlan:\n${plan.treeString}") {
        result.size should be >= 2
      }
      result.foreach { case (stageId, nodes) =>
        withClue(s"Stage $stageId should have >= 1 node") {
          nodes should not be empty
        }
      }
    } finally {
      sparkInstrumented.conf.set("spark.sql.autoBroadcastJoinThreshold", prevThreshold)
    }
  }

  // ---------------------------------------------------------------------------
  // Test 4: nodes without timing get exclusiveMs = 0
  // ---------------------------------------------------------------------------
  test("nodes without timing metrics get exclusiveMs = 0") {
    val df = sparkInstrumented.range(0, 100, 1, 1).toDF().filter("id > 50")
    df.collect()

    val all = allNodeDurations(PlanDurationAttribution.compute(executedPlan(df)))
    val untimed = all.filter(nd => !nd.node.isInstanceOf[TimedExec] && nd.node.getClass.getSimpleName != "WholeStageCodegenExec")

    withClue("Expected at least one non-TimedExec node (e.g. RangeExec)") {
      untimed should not be empty
    }
    untimed.foreach { nd =>
      withClue(s"${nd.node.getClass.getSimpleName} should have exclusiveMs == 0") {
        nd.exclusiveMs shouldBe 0L
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Test 5: native exclusive metric — SortExec.sortTime
  // (These tests stop sparkInstrumented and create a fresh native session)
  // ---------------------------------------------------------------------------
  test("native exclusive metric: SortExec.sortTime used directly") {
    val sparkNative = createNativeSession()
    val df = sparkNative.range(0, 10000, 1, 1).toDF("id").sort(col("id").desc)
    df.collect()

    val all = allNodeDurations(PlanDurationAttribution.compute(executedPlan(df)))
    val sortND = all.find(_.node.getClass.getSimpleName == "SortExec")

    withClue(s"Expected SortExec in plan:\n${executedPlan(df).treeString}") {
      sortND shouldBe defined
    }

    val nd = sortND.get
    withClue(s"SortExec exclusiveMs=${nd.exclusiveMs} should be > 0") {
      nd.exclusiveMs should be > 0L
    }

    val sortTimeMetric = nd.node.metrics.get("sortTime")
    withClue("SortExec should have sortTime metric") {
      sortTimeMetric shouldBe defined
    }
    withClue("SortExec exclusiveMs should equal sortTime metric value") {
      nd.exclusiveMs shouldBe sortTimeMetric.get.value
    }
  }

  // ---------------------------------------------------------------------------
  // Test 6: native exclusive metric — HashAggregateExec.aggTime
  // ---------------------------------------------------------------------------
  test("native exclusive metric: HashAggregateExec.aggTime used directly") {
    // Session may already be native from test 5, or we need to create it
    val sparkNative = SparkSession.getActiveSession.getOrElse(createNativeSession())
    val df = sparkNative.range(0, 10000, 1, 1).toDF("id").groupBy("id").count()
    df.collect()

    val all = allNodeDurations(PlanDurationAttribution.compute(executedPlan(df)))
    val aggND = all.find(_.node.getClass.getSimpleName == "HashAggregateExec")

    withClue(s"Expected HashAggregateExec in plan:\n${executedPlan(df).treeString}") {
      aggND shouldBe defined
    }

    val nd = aggND.get
    withClue(s"HashAggregateExec exclusiveMs=${nd.exclusiveMs} should be > 0") {
      nd.exclusiveMs should be > 0L
    }

    val aggTimeMetric = nd.node.metrics.get("aggTime")
    withClue("HashAggregateExec should have aggTime metric") {
      aggTimeMetric shouldBe defined
    }
    withClue("HashAggregateExec exclusiveMs should equal aggTime metric value") {
      nd.exclusiveMs shouldBe aggTimeMetric.get.value
    }
  }
}
