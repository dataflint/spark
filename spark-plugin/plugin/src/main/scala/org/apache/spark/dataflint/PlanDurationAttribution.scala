package org.apache.spark.dataflint

import org.apache.spark.sql.execution.SparkPlan


/**
 * Traverses an executed physical plan and computes the attributed (exclusive) duration
 * per node, grouped by SQL stage.
 *
 * ## Completely decoupled from TimedExec
 *   Attribution is purely metric-reading. Any node that exposes a timing metric —
 *   whether via TimedExec.duration, SortExec.sortTime, HashAggregateExec.aggTime,
 *   or any other key — participates in the model uniformly. TimedExec is just one
 *   instrumentation vehicle; the attribution logic does not reference it.
 *
 * ## Stage definition
 *   A SQL stage is a subtree separated by Exchange boundaries (ShuffleExchangeExec,
 *   BroadcastExchangeExec, AQE QueryStageExec). Stage IDs are assigned depth-first.
 *
 * ## Attribution model
 *   Node N with pipelined child C (same stage, both have timing):
 *     exclusive(N) = D(N) - D(C)        // C's time is contained inside N's wall clock
 *
 *   Node N with blocking child B (same stage):
 *     exclusive(N) = D(N)               // B materialized before N started — sequential
 *
 *   Node with no timing metric:
 *     exclusive(N) = 0                  // unobservable without instrumentation
 *
 * ## Class-name-based classification
 *   All node classification (blocking, exchange, native metric keys) uses
 *   getClass.getSimpleName — no direct class imports, resilient to Spark version changes
 *   and usable with custom operators.
 *
 * ## Usage
 *   val attribution = PlanDurationAttribution.compute(executedPlan)
 *   attribution.toSeq.sortBy(_._1).foreach { case (stageId, nodes) =>
 *     println(s"--- Stage $stageId ---")
 *     nodes.foreach { nd =>
 *       println(f"  ${nd.node.nodeName}%-40s ${nd.exclusiveMs}%6d ms")
 *     }
 *   }
 */
object PlanDurationAttribution {

  /** Attributed exclusive duration for one physical operator. */
  case class NodeDuration(node: SparkPlan, exclusiveMs: Long)

  // -----------------------------------------------------------------------------------------
  // Classification tables — all by getClass.getSimpleName, no class imports needed
  // -----------------------------------------------------------------------------------------

  /**
   * Blocking operators: fully materialize input before emitting any row.
   * Two consecutive blockers in the same stage are sequential — never subtract across them.
   * Extend this set to cover custom operators.
   */
  private val BlockingNodes: Set[String] = Set(
    "SortExec",
    "HashAggregateExec",
    "ObjectHashAggregateExec",
    "SortAggregateExec",
    "SortMergeJoinExec",
    "ShuffledHashJoinExec",
    "BroadcastHashJoinExec",
    "BroadcastNestedLoopJoinExec",
    "CartesianProductExec",
    "WindowExec"
  )

  /**
   * Exchange / stage-boundary nodes: children execute in a prior stage.
   * Walking stops here; child subtrees are recursed into with a fresh stage ID.
   */
  private val ExchangeNodes: Set[String] = Set(
    "ShuffleExchangeExec",
    "BroadcastExchangeExec",
    "ShuffleQueryStageExec",
    "BroadcastQueryStageExec",
    "ResultQueryStageExec"
  )

  /**
   * Native exclusive metric keys per class: these metrics already measure only the
   * node's own work (child pull time excluded), so they are used directly as exclusive
   * duration without any subtraction.
   *
   * Priority order: first match wins.
   */
  private val NativeExclusiveMetrics: Map[String, Seq[String]] = Map(
    "SortExec"               -> Seq("sortTime"),
    "HashAggregateExec"      -> Seq("aggTime"),
    "ObjectHashAggregateExec"-> Seq("aggTime"),
    "ShuffledHashJoinExec"   -> Seq("buildTime")
  )

  /**
   * Timing metric keys to read from any node — in priority order.
   * The first key found in node.metrics is used as D(N).
   * Covers both TimedExec-injected "duration" and any native timing metric.
   */
  private val TimingMetricKeys: Seq[String] = Seq(
    "duration",    // TimedExec
    "sortTime",    // SortExec
    "aggTime",     // HashAggregateExec, ObjectHashAggregateExec
    "buildTime",   // ShuffledHashJoinExec
    "pipelineTime" // WholeStageCodegenExec (fallback — covers entire stage)
  )

  // -----------------------------------------------------------------------------------------
  // Entry point
  // -----------------------------------------------------------------------------------------

  /**
   * Compute attributed exclusive durations for every node in the plan, grouped by stage.
   *
   * @param root executedPlan root — must have been fully executed (action completed)
   * @return Map[stageId -> Seq[NodeDuration]], nodes in top-down order within each stage
   */
  def compute(root: SparkPlan): Map[Int, Seq[NodeDuration]] = {
    val counter = new java.util.concurrent.atomic.AtomicInteger(0)
    val result  = scala.collection.mutable.Map.empty[Int, Seq[NodeDuration]]

    def walk(plan: SparkPlan, stageId: Int): Unit = {
      val className = plan.getClass.getSimpleName

      if (ExchangeNodes.contains(className)) {
        // Exchange boundary — each child starts a fresh stage
        plan.children.foreach { c => walk(c, counter.incrementAndGet()) }
      } else {
        // Collect and attribute all nodes within this stage boundary
        val stageNodes = collectStageNodes(plan)
        result(stageId) = result.getOrElse(stageId, Seq.empty) ++ attributeStage(stageNodes)

        // Recurse into Exchange boundaries reachable from ANY node in this stage
        stageNodes
          .flatMap(_.children)
          .filter(c => ExchangeNodes.contains(c.getClass.getSimpleName))
          .distinct
          .foreach { c => walk(c, counter.incrementAndGet()) }
      }
    }

    walk(root, counter.incrementAndGet())
    result.toMap
  }

  // -----------------------------------------------------------------------------------------
  // Stage node collection
  // -----------------------------------------------------------------------------------------

  /**
   * Depth-first collection of all SparkPlan nodes within one stage.
   * Stops at Exchange boundaries — those belong to child stages.
   * Returns nodes in top-down order.
   */
  private def collectStageNodes(root: SparkPlan): Seq[SparkPlan] = {
    val buf = scala.collection.mutable.ArrayBuffer.empty[SparkPlan]

    def collect(plan: SparkPlan): Unit = {
      if (ExchangeNodes.contains(plan.getClass.getSimpleName)) return
      buf += plan
      plan.children.foreach(collect)
    }

    collect(root)
    buf.toSeq
  }

  // -----------------------------------------------------------------------------------------
  // Attribution computation
  // -----------------------------------------------------------------------------------------

  /**
   * Apply the attribution model to an ordered (top-down) list of nodes in one stage.
   *
   * For each node N:
   *   1. Read D(N) from its timing metrics (first TimingMetricKey found).
   *   2. If N has a native exclusive metric (sortTime, aggTime, etc.) — use it directly.
   *   3. If N is blocking — D(N) is exclusive (no subtraction).
   *   4. If N is pipelined — find the nearest pipelined timing child C:
   *        exclusive(N) = max(0, D(N) - D(C))
   *   5. No timing metric — exclusive = 0.
   */
  private def attributeStage(nodes: Seq[SparkPlan]): Seq[NodeDuration] = {
    nodes.map { node =>
      val className   = node.getClass.getSimpleName
      val exclusiveMs = nativeExclusiveMs(node, className) match {

        case Some(native) =>
          // Node has a pre-attributed native metric — use it directly
          native

        case None =>
          readTimingMs(node) match {
            case None => 0L  // no timing at all — unobservable

            case Some(selfMs) =>
              if (BlockingNodes.contains(className)) {
                // Blocking: duration is sequential with child, already exclusive
                selfMs
              } else {
                // Pipelined: subtract nearest pipelined timing child in same stage
                findPipelinedTimedChild(node, nodes) match {
                  case Some(childMs) => math.max(0L, selfMs - childMs)
                  case None          => selfMs
                }
              }
          }
      }
      NodeDuration(node, exclusiveMs)
    }
  }

  /**
   * Walk node's children looking for the nearest descendant that:
   *   - has a timing metric (any TimingMetricKey), AND
   *   - is reachable without crossing a blocking or exchange boundary
   *
   * Returns the timing value of that descendant, or None.
   */
  private def findPipelinedTimedChild(
      node: SparkPlan,
      allStageNodes: Seq[SparkPlan]
  ): Option[Long] = {
    val stageNodeSet = allStageNodes.toSet

    def search(plan: SparkPlan): Option[Long] =
      plan.children.flatMap { child =>
        val cn = child.getClass.getSimpleName
        if (ExchangeNodes.contains(cn) || !stageNodeSet.contains(child)) {
          None
        } else if (BlockingNodes.contains(cn)) {
          // Stop — blocking child is sequential, not contained in parent's clock
          None
        } else {
          readTimingMs(child) match {
            case some @ Some(_) => some          // found a timed pipelined child
            case None           => search(child) // transparent node — look deeper
          }
        }
      }.headOption

    search(node)
  }

  // -----------------------------------------------------------------------------------------
  // Metric reading helpers
  // -----------------------------------------------------------------------------------------

  /**
   * Read the first available timing metric from a node, in TimingMetricKeys priority order.
   * Returns None if no timing metric is present.
   */
  private def readTimingMs(node: SparkPlan): Option[Long] =
    TimingMetricKeys
      .flatMap(node.metrics.get)
      .map(_.value)
      .headOption

  /**
   * If the node has a known native exclusive metric (already exclusive by definition),
   * return it directly, bypassing the subtraction logic entirely.
   */
  private def nativeExclusiveMs(node: SparkPlan, className: String): Option[Long] =
    NativeExclusiveMetrics
      .get(className)
      .flatMap { keys =>
        keys.flatMap(node.metrics.get).map(_.value).headOption
      }
}
