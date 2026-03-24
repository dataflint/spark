package org.apache.spark.dataflint

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric

/**
 * A generic SparkPlan wrapper that adds a `duration` metric to any node while preserving
 * all of the child's existing metrics (spillSize, numOutputRows, etc.).
 *
 * ## Single node in Spark plan graph
 * `children` is set to `child.children` (the wrapped node's own children), so `TimedExec`
 * appears as ONE node in the Spark plan tree and `SparkPlanGraph`. The wrapped `child` is
 * not a separate visible node, eliminating the "double node" problem in both Spark's native
 * SQL UI and the DataFlint UI.
 *
 * ## Node name
 * `nodeName` is prefixed with "DataFlint" so the node is clearly identified in plan displays.
 *
 * ## Plan reconstruction (Spark 3.2+)
 * When Spark updates our children (= child.children) via plan transformations, we rebuild
 * `child` with the new grandchildren via `child.withNewChildren(newChildren)`, then wrap
 * the rebuilt child in a new `TimedExec`.
 *
 * ## Spark 3.0/3.1 compatibility
 * Extends SparkPlan directly (not UnaryExecNode) to avoid a `UnaryLike` reference in
 * compiled bytecode, which would cause NoClassDefFoundError on Spark 3.0/3.1.
 * productElement/productArity support `makeCopy` on Spark 3.0/3.1.
 */
class TimedExec(val child: SparkPlan) extends SparkPlan with Logging {
  override def nodeName: String = "DataFlint" + child.nodeName
  override def output: Seq[Attribute] = child.output

  // Expose child's children directly so TimedExec appears as a single node in the plan graph.
  // The wrapped child is not visible in the tree; plan transformations see and update the
  // grandchildren directly, and withNewChildrenInternal rebuilds child with the new ones.
  override def children: Seq[SparkPlan] = child.children

  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  // Preserves ALL of child's existing metrics (spillSize, numOutputRows, etc.) + adds duration
  override lazy val metrics: Map[String, SQLMetric] =
    child.metrics ++ Map(MetricsUtils.getTimingMetric("duration")(sparkContext))

  override protected def doExecute(): RDD[InternalRow] =
    DataFlintRDDUtils.withDurationMetric(child.execute(), longMetric("duration"))

  override def canEqual(that: Any): Boolean = that.isInstanceOf[TimedExec]
  // productArity/productElement support makeCopy on Spark 3.0/3.1 (constructor arg = child)
  override def productArity: Int = 1
  override def productElement(n: Int): Any =
    if (n == 0) child else throw new IndexOutOfBoundsException(s"$n")

  // When Spark updates our children (= child's children), rebuild child with new children
  // and wrap in a new TimedExec. Used by Spark 3.2+ plan transformations (AQE, CollapseCodegen, etc.)
  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan =
    new TimedExec(child.withNewChildren(newChildren))
}

object TimedExec {
  def apply(child: SparkPlan): TimedExec = new TimedExec(child)
}