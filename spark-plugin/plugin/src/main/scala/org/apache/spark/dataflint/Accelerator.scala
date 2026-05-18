package org.apache.spark.dataflint

/**
 * A native query accelerator that replaces Spark physical operators with its own variants.
 *
 * Used by [[DataFlintInstrumentationColumnarRule]] to decide which operators to skip when
 * wrapping with [[TimedExec]]. `TimedExec` is transparent on Spark 3.2+ (`children =
 * child.children`), so wrapping a Spark operator hides it from any other plan-transform
 * rule — including the accelerator's own substitution rule, which then silently falls back
 * to the Spark JVM path.
 *
 * @param name             human-readable label for log messages
 * @param detectionClasses class names checked via reflection; if ANY is on the classpath
 *                         the accelerator is considered active
 * @param transformableExecs simple class names of Spark `*Exec` operators this accelerator
 *                         will replace during physical planning. DataFlint must not wrap
 *                         these.
 */
case class Accelerator(
    name: String,
    detectionClasses: Seq[String],
    transformableExecs: Set[String])

object Accelerator {
  // Gluten/Velox — Apache Gluten project (Velox native engine).
  val GlutenVelox: Accelerator = Accelerator(
    name = "Gluten/Velox",
    detectionClasses = Seq("org.apache.gluten.GlutenPlugin"),
    transformableExecs = Set(
      "FilterExec", "ProjectExec", "SortExec", "ExpandExec", "GenerateExec",
      "HashAggregateExec", "SortAggregateExec", "ObjectHashAggregateExec",
      "SortMergeJoinExec", "BroadcastHashJoinExec", "ShuffledHashJoinExec",
      "BroadcastNestedLoopJoinExec", "CartesianProductExec",
      "WindowExec", "WindowGroupLimitExec",
      "FileSourceScanExec", "BatchScanExec",
    ))

  // Comet — Apache DataFusion Comet engine.
  val CometDataFusion: Accelerator = Accelerator(
    name = "Comet/DataFusion",
    detectionClasses = Seq(
      "org.apache.spark.CometPlugin",
      "org.apache.comet.CometSparkSessionExtensions"),
    transformableExecs = Set(
      "FilterExec", "ProjectExec", "SortExec", "ExpandExec",
      "HashAggregateExec", "ObjectHashAggregateExec",
      "SortMergeJoinExec", "BroadcastHashJoinExec", "ShuffledHashJoinExec",
      "FileSourceScanExec", "BatchScanExec",
    ))

  // RAPIDS — NVIDIA RAPIDS Accelerator for Apache Spark.
  val RapidsGpu: Accelerator = Accelerator(
    name = "RAPIDS GPU",
    detectionClasses = Seq(
      "com.nvidia.spark.SQLPlugin",
      "com.nvidia.spark.rapids.RapidsPlugin"),
    transformableExecs = Set(
      "FilterExec", "ProjectExec", "SortExec", "ExpandExec", "GenerateExec",
      "HashAggregateExec", "SortAggregateExec", "ObjectHashAggregateExec",
      "BroadcastHashJoinExec", "ShuffledHashJoinExec", "SortMergeJoinExec",
      "BroadcastNestedLoopJoinExec",
      "WindowExec", "WindowGroupLimitExec",
      "FileSourceScanExec", "BatchScanExec",
    ))

  // Photon — Databricks proprietary engine. Photon is only on the DBR classpath, so
  // probing for Photon-internal classes is safe on a normal Spark distribution.
  val Photon: Accelerator = Accelerator(
    name = "Photon",
    detectionClasses = Seq(
      "com.databricks.photon.PhotonAdaptiveSparkPlanExec",
      "com.databricks.sql.execution.PhotonResultStage"),
    transformableExecs = Set(
      "FilterExec", "ProjectExec",
      "HashAggregateExec", "SortAggregateExec", "ObjectHashAggregateExec",
      "BroadcastHashJoinExec", "ShuffledHashJoinExec", "SortMergeJoinExec",
      "BroadcastNestedLoopJoinExec",
      "WindowExec",
      "FileSourceScanExec",
    ))

  val known: Seq[Accelerator] = Seq(GlutenVelox, CometDataFusion, RapidsGpu, Photon)

  private def isClassPresent(className: String): Boolean = {
    try {
      Class.forName(className, false, Thread.currentThread().getContextClassLoader)
      true
    } catch {
      case _: ClassNotFoundException | _: NoClassDefFoundError => false
    }
  }

  /**
   * Accelerators detected on the current classpath. Probed lazily; the result is cached
   * for the JVM lifetime since classpath composition cannot change after startup.
   */
  lazy val active: Seq[Accelerator] =
    known.filter(_.detectionClasses.exists(isClassPresent))
}