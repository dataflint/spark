package org.apache.spark.dataflint

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.ui.{SQLAppStatusStore, SparkPlanGraph}
import org.apache.spark.sql.functions.{col, row_number}
import org.apache.spark.sql.expressions.Window
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class GraphStageGroupSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("GraphStageGroupSpec")
      .config("spark.sql.codegen.wholeStage", "true")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.ui.enabled", "true")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
  }

  private def graphFor(df: DataFrame): SparkPlanGraph = {
    df.collect()
    val sqlStore = new SQLAppStatusStore(spark.sparkContext.statusStore.store, None)
    sqlStore.planGraph(sqlStore.executionsList().last.executionId)
  }

  test("no shuffle produces exactly 1 stage group") {
    val groups = GraphDurationAttribution.computeStageGroups(graphFor(
      spark.range(0, 100, 1, 1).toDF().filter("id > 50")
    ))
    groups.size shouldBe 1
    groups.head.nodeIds should not be empty
    groups.head.exchangeBoundaries shouldBe empty
  }

  test("single exchange produces exactly 2 stage groups") {
    val groups = GraphDurationAttribution.computeStageGroups(graphFor(
      spark.range(0, 100, 1, 1).toDF("id").repartition(2)
    ))
    groups.size shouldBe 2
    groups.filter(_.exchangeBoundaries.nonEmpty).size shouldBe 1
  }

  test("chained exchanges (repartition + window) produce exactly 3 stage groups") {
    val df = spark.range(0, 100, 1, 1).toDF("id")
      .withColumn("group", col("id") % 10)
      .repartition(4)
      .withColumn("rn", row_number().over(Window.partitionBy("group").orderBy("id")))
    val groups = GraphDurationAttribution.computeStageGroups(graphFor(df))
    groups.size shouldBe 3
  }

  test("node IDs are unique across all groups") {
    val df = spark.range(0, 100, 1, 1).toDF("id").repartition(2).select(col("id") + 1)
    val allNodeIds = GraphDurationAttribution.computeStageGroups(graphFor(df)).flatMap(_.nodeIds)
    allNodeIds.distinct.size shouldBe allNodeIds.size
  }
}