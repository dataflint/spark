package org.apache.spark.dataflint

import org.apache.spark.deploy.PythonRunner
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SparkPlan
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.Paths

/**
 * Integration test that runs a Python script against the instrumented SparkSession.
 *
 * PythonRunner.main() creates a Py4JServer internally and sets PYSPARK_GATEWAY_PORT
 * for the subprocess — no manual gateway setup needed. The Python script then connects
 * to this JVM via launch_gateway() and accesses the session through DataFlintStaticSession.
 *
 * Requires: .venv with pyspark, pandas, pyarrow installed.
 *   python3 -m venv .venv && .venv/bin/pip install pyspark pandas pyarrow
 */
class DataFlintPythonIntegrationSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll with DataFlintTestHelper {

  // pluginspark3 tests run with CWD = spark-plugin/pluginspark3/, so go up one level
  // to reach the project root where .venv and pyspark-testing live.
  private val projectRoot = Paths.get("").toAbsolutePath//.getParent

  private val venvPython: String = {
    val p = projectRoot.resolve(Paths.get(".venv", "bin", "python3"))
    require(p.toFile.exists(),
      s"Python venv not found at $p\n" +
      "Run: python3 -m venv .venv && .venv/bin/pip install pyspark pandas pyarrow")
    p.toString
  }

  private val scriptPath: String =
    projectRoot.resolve(
      Paths.get("pyspark-testing", "dataflint_python_exec_integration_test.py")).toString

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    // Set before SparkSession creation so the conf is also applied to UDF workers.
    // Also sets it as a system property so PythonRunner's internal `new SparkConf()`
    // picks it up when resolving the Python executable for the subprocess.
    System.setProperty("spark.pyspark.python", venvPython)

    spark = SparkSession.builder()
      .master("local[2]")
      .appName("DataFlintPythonIntegrationSpec")
      .config("spark.pyspark.python",                                                 venvPython)
      .config(DataflintSparkUICommonLoader.INSTRUMENT_SPARK_ENABLED,                  "true")
      .config(DataflintSparkUICommonLoader.INSTRUMENT_ARROW_EVAL_PYTHON_ENABLED,      "true")
      .config(DataflintSparkUICommonLoader.INSTRUMENT_BATCH_EVAL_PYTHON_ENABLED,      "true")
      .config(DataflintSparkUICommonLoader.INSTRUMENT_FLAT_MAP_GROUPS_PANDAS_ENABLED,   "true")
      .config(DataflintSparkUICommonLoader.INSTRUMENT_FLAT_MAP_COGROUPS_PANDAS_ENABLED, "true")
      .config("spark.ui.enabled", "false")
      .withExtensions(new DataFlintInstrumentationExtension)
      .getOrCreate()

    DataFlintStaticSession.set(spark)
  }

  override def afterAll(): Unit = {
    DataFlintStaticSession.clear()
    System.clearProperty("spark.pyspark.python")
    if (spark != null) spark.stop()
  }

  /** Run the Python script for the given test name, then check the registered view. */
  private def assertPythonNode(testName: String, view: String, expectedNode: String): Unit = {
    PythonRunner.main(Array(scriptPath, "", testName))
    val df = spark.table(view)
    df.collect()
    val node = finalPlan(df).collect {
      case n: SparkPlan if n.getClass.getSimpleName.contains(expectedNode) => n
    }.headOption
    node shouldBe defined
    node.get.metrics should contain key "duration"
    node.get.metrics("duration").value should be > 0L
  }

  test("BatchEvalPython (@udf) is instrumented") {
    assertPythonNode("batch_eval", "batch_eval_view", "DataFlintBatchEvalPython")
  }

  test("ArrowEvalPython (@pandas_udf scalar) is instrumented") {
    assertPythonNode("arrow_eval", "arrow_eval_view", "DataFlintArrowEvalPython")
  }

  test("FlatMapGroupsInPandas (applyInPandas) is instrumented") {
    assertPythonNode("flat_map_groups", "flat_map_groups_view", "DataFlintFlatMapGroupsInPandas")
  }

  test("FlatMapCoGroupsInPandas (cogroup applyInPandas) is instrumented") {
    assertPythonNode("flat_map_cogroups", "flat_map_cogroups_view", "DataFlintFlatMapCoGroupsInPandas")
  }
}