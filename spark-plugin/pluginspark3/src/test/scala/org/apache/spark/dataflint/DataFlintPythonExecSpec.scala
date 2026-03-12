package org.apache.spark.dataflint

import org.apache.spark.api.python.{PythonEvalType, SimplePythonFunction}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.PythonUDF
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.execution.python._
import org.apache.spark.sql.types.LongType
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DataFlintPythonExecSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("DataFlintPythonExecSpec")
      .config(DataflintSparkUICommonLoader.INSTRUMENT_SPARK_ENABLED, "true")
      .withExtensions(new DataFlintInstrumentationExtension)
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  private def emptyChild = LocalTableScanExec(output = Seq.empty, rows = Seq.empty)

  private def fakePythonUDF(evalType: Int): PythonUDF = {
    val func = SimplePythonFunction(
      command       = Seq.empty[Byte],
      envVars       = new java.util.HashMap[String, String](),
      pythonIncludes = new java.util.ArrayList[String](),
      pythonExec    = "python3",
      pythonVer     = "3.8",
      broadcastVars = new java.util.ArrayList(),
      accumulator   = null)
    PythonUDF(
      name             = "test_udf",
      func             = func,
      dataType         = LongType,
      children         = Seq.empty,
      evalType         = evalType,
      udfDeterministic = true)
  }

  // ---- MapInPandasExec (mapInPandas) ----

  test("replaces MapInPandasExec in plan") {
    val udf = fakePythonUDF(PythonEvalType.SQL_MAP_PANDAS_ITER_UDF)
    val original = MapInPandasExec(udf, Seq.empty, emptyChild, false)
    val rule = DataFlintInstrumentationColumnarRule(spark)
    val result = rule.preColumnarTransitions(original)
    result shouldBe a[DataFlintMapInPandasExec_3_5]
  }

  test("MapInPandasExec nodeName and metrics") {
    val udf = fakePythonUDF(PythonEvalType.SQL_MAP_PANDAS_ITER_UDF)
    val original = MapInPandasExec(udf, Seq.empty, emptyChild, false)
    val rule = DataFlintInstrumentationColumnarRule(spark)
    val result = rule.preColumnarTransitions(original).asInstanceOf[DataFlintMapInPandasExec_3_5]
    result.nodeName shouldBe "DataFlintMapInPandas"
    result.metrics should contain key "duration"
  }

  // ---- PythonMapInArrowExec (mapInArrow) ----

  test("replaces PythonMapInArrowExec in plan") {
    val udf = fakePythonUDF(PythonEvalType.SQL_MAP_ARROW_ITER_UDF)
    val original = PythonMapInArrowExec(udf, Seq.empty, emptyChild, false)
    val rule = DataFlintInstrumentationColumnarRule(spark)
    val result = rule.preColumnarTransitions(original)
    result shouldBe a[DataFlintPythonMapInArrowExec_3_5]
  }

  test("PythonMapInArrowExec nodeName and metrics") {
    val udf = fakePythonUDF(PythonEvalType.SQL_MAP_ARROW_ITER_UDF)
    val original = PythonMapInArrowExec(udf, Seq.empty, emptyChild, false)
    val rule = DataFlintInstrumentationColumnarRule(spark)
    val result = rule.preColumnarTransitions(original).asInstanceOf[DataFlintPythonMapInArrowExec_3_5]
    result.nodeName shouldBe "DataFlintMapInArrow"
    result.metrics should contain key "duration"
  }

  // ---- ArrowEvalPythonExec (pandas_udf SCALAR) ----

  test("replaces ArrowEvalPythonExec in plan") {
    val udf = fakePythonUDF(PythonEvalType.SQL_SCALAR_PANDAS_UDF)
    val original = ArrowEvalPythonExec(Seq(udf), Seq.empty, emptyChild, PythonEvalType.SQL_SCALAR_PANDAS_UDF)
    val rule = DataFlintInstrumentationColumnarRule(spark)
    val result = rule.preColumnarTransitions(original)
    result shouldBe a[DataFlintArrowEvalPythonExec_3_2]
  }

  test("ArrowEvalPythonExec nodeName and metrics") {
    val udf = fakePythonUDF(PythonEvalType.SQL_SCALAR_PANDAS_UDF)
    val original = ArrowEvalPythonExec(Seq(udf), Seq.empty, emptyChild, PythonEvalType.SQL_SCALAR_PANDAS_UDF)
    val rule = DataFlintInstrumentationColumnarRule(spark)
    val result = rule.preColumnarTransitions(original).asInstanceOf[DataFlintArrowEvalPythonExec_3_2]
    result.nodeName shouldBe "DataFlintArrowEvalPython"
    result.metrics should contain key "duration"
  }

  // ---- FlatMapGroupsInPandasExec (applyInPandas / GROUPED_MAP) ----

  test("replaces FlatMapGroupsInPandasExec in plan") {
    val udf = fakePythonUDF(PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF)
    val original = FlatMapGroupsInPandasExec(Seq.empty, udf, Seq.empty, emptyChild)
    val rule = DataFlintInstrumentationColumnarRule(spark)
    val result = rule.preColumnarTransitions(original)
    result shouldBe a[DataFlintFlatMapGroupsInPandasExec]
  }

  test("FlatMapGroupsInPandasExec nodeName and metrics") {
    val udf = fakePythonUDF(PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF)
    val original = FlatMapGroupsInPandasExec(Seq.empty, udf, Seq.empty, emptyChild)
    val rule = DataFlintInstrumentationColumnarRule(spark)
    val result = rule.preColumnarTransitions(original).asInstanceOf[DataFlintFlatMapGroupsInPandasExec]
    result.nodeName shouldBe "DataFlintFlatMapGroupsInPandas"
    result.metrics should contain key "duration"
  }

  // ---- FlatMapCoGroupsInPandasExec (cogroup / applyInPandas on two DataFrames) ----

  test("replaces FlatMapCoGroupsInPandasExec in plan") {
    val udf = fakePythonUDF(PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF)
    val original = FlatMapCoGroupsInPandasExec(Seq.empty, Seq.empty, udf, Seq.empty, emptyChild, emptyChild)
    val rule = DataFlintInstrumentationColumnarRule(spark)
    val result = rule.preColumnarTransitions(original)
    result shouldBe a[DataFlintFlatMapCoGroupsInPandasExec]
  }

  test("FlatMapCoGroupsInPandasExec nodeName and metrics") {
    val udf = fakePythonUDF(PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF)
    val original = FlatMapCoGroupsInPandasExec(Seq.empty, Seq.empty, udf, Seq.empty, emptyChild, emptyChild)
    val rule = DataFlintInstrumentationColumnarRule(spark)
    val result = rule.preColumnarTransitions(original).asInstanceOf[DataFlintFlatMapCoGroupsInPandasExec]
    result.nodeName shouldBe "DataFlintFlatMapCoGroupsInPandas"
    result.metrics should contain key "duration"
  }
}
