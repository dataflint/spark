package org.apache.spark.dataflint

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.ui.{SQLAppStatusListener, SQLAppStatusStore, SparkPlanGraph}
import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scala.collection.JavaConverters._

class GraphDurationAttributionSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private lazy val sqlStore: SQLAppStatusStore = {
    val listener = spark.sparkContext.listenerBus.listeners.asScala
      .collectFirst { case l: SQLAppStatusListener => l }
    new SQLAppStatusStore(spark.sparkContext.statusStore.store, listener)
  }

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("GraphDurationAttributionSpec")
      .config(DataflintSparkUICommonLoader.INSTRUMENT_SQL_NODES_ENABLED, "true")
      .config("spark.sql.codegen.wholeStage", "true")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.ui.enabled", "true")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.dataflint.test.codegenSleepMs", "2")
      .withExtensions(new DataFlintInstrumentationExtension)
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
  }

  private def graphForQuery(df: DataFrame): (SparkPlanGraph, Map[Long, String]) = {
    df.collect()
    val lastExec = sqlStore.executionsList().last
    (sqlStore.planGraph(lastExec.executionId), sqlStore.executionMetrics(lastExec.executionId))
  }

  private def resolvedForQuery(df: DataFrame): (Seq[api.ResolvedStageGroup], Seq[GraphDurationAttribution.NodeDuration]) = {
    val (graph, metrics) = graphForQuery(df)
    val lastExec = sqlStore.executionsList().last
    val sqlStageIds = lastExec.stages.map(_.intValue()).toSet
    val stageInfos = spark.sparkContext.statusStore.stageList(null)
      .filter(s => sqlStageIds.contains(s.stageId))
      .map(s => GraphDurationAttribution.StageInfo(s.stageId, s.status.toString, s.numTasks, s.numCompleteTasks, s.executorRunTime))
    val resolved = GraphDurationAttribution.resolveStageGroups(graph, metrics, sqlStageIds, stageInfos)
    val durations = GraphDurationAttribution.computeAutoNormalized(graph, metrics, resolved)
    (resolved, durations)
  }

  // --- Duration attribution ---

  test("durations: all non-negative, instrumented nodes have measurable timing") {
    val df = spark.range(0, 1000, 1, 1).toDF().filter("id > 50")
    val (graph, metrics) = graphForQuery(df)
    val durations = GraphDurationAttribution.computeAuto(graph, metrics)

    durations.foreach(d => d.durationMs should be >= 0L)
    val nonZero = durations.filter(_.durationMs > 0)
    withClue(s"Durations: ${durations.map(d => s"${d.nodeName}:${d.durationMs}ms").mkString(", ")}") {
      nonZero should not be empty
    }
  }

  // --- Exchange durations ---

  test("exchange durations: write and read are non-negative, at least one exchange found") {
    val df = spark.range(0, 1000, 1, 1).toDF("id").repartition(2).select(col("id") + 1)
    val (graph, metrics) = graphForQuery(df)
    val exchangeDurations = GraphDurationAttribution.computeExchangeDurations(graph, metrics)

    exchangeDurations should not be empty
    exchangeDurations.foreach { d =>
      withClue(s"Exchange(${d.nodeId})") {
        d.writeDurationMs should be >= 0L
        d.readDurationMs should be >= 0L
      }
    }
  }

  // --- Resolved groups + normalization ---

  test("resolved groups: valid sparkStageIds, exchange in write+read, metadata populated") {
    val (resolved, _) = resolvedForQuery(spark.range(0, 100, 1, 1).toDF("id").repartition(2))

    resolved.size shouldBe 2
    resolved.foreach(g => g.sparkStageId should not be -1)
    resolved.foreach(g => g.numTasks should be > 0)

    val reads = resolved.flatMap(_.exchangeReadIds).toSet
    val writes = resolved.flatMap(_.exchangeWriteIds).toSet
    reads.intersect(writes) should not be empty
  }

  test("normalized durations: non-negative and within stage executorRunTime") {
    val df = spark.range(0, 1000, 1, 1).toDF().filter("id > 50").select(col("id") + 1)
    val (resolved, durations) = resolvedForQuery(df)
    val durationMap = durations.map(d => d.nodeId -> d.durationMs).toMap

    durations.foreach(d => d.durationMs should be >= 0L)
    for (group <- resolved if group.executorRunTime > 0 && group.nodeIds.nonEmpty) {
      val stageSum = group.nodeIds.map(id => durationMap.getOrElse(id, 0L)).sum
      stageSum should be <= (group.executorRunTime + group.nodeIds.size)
    }
  }
}