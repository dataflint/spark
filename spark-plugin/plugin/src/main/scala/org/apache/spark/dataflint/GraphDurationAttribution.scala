package org.apache.spark.dataflint

import org.apache.spark.sql.execution.ui.{SparkPlanGraph, SparkPlanGraphCluster, SparkPlanGraphEdge, SparkPlanGraphNode}

/**
 * Computes stage groups and duration attribution from a SparkPlanGraph + metric values.
 *
 * Unlike PlanDurationAttribution (which requires the live SparkPlan tree),
 * this works with the persisted SparkPlanGraph from the status store — available
 * in both live execution and history server.
 *
 * ## Data sources
 *   - SparkPlanGraph: nodes (with names, metric definitions), edges (parent-child)
 *   - metricValues: Map[accumulatorId -> value string] from SQLAppStatusStore.executionMetrics()
 *
 * ## Stage grouping (topology-only, no metrics needed)
 *   Exchange nodes are boundaries. Nodes between exchanges belong to the same stage group.
 *   Groups get sequential planStageIds (1, 2, 3...) which can later be mapped to Spark stage IDs.
 *
 * ## Duration attribution
 *   Same model as PlanDurationAttribution:
 *   - Native exclusive metrics (sort time, agg time, build time) used directly
 *   - Pipelined: exclusive = inclusive - child inclusive
 *   - Blocking: exclusive = inclusive
 *   - Zero-duration nodes are transparent
 *
 * ## Node classification uses display names ("Sort", "Exchange", etc.)
 *   not class names ("SortExec", "ShuffleExchangeExec").
 */
object GraphDurationAttribution {

  case class NodeDuration(nodeId: Long, nodeName: String, durationMs: Long)
  case class ExchangeDuration(nodeId: Long, writeDurationMs: Long, readDurationMs: Long)

  case class StageGroup(
    planStageId: Int,
    nodeIds: Seq[Long],
    exchangeBoundaries: Seq[(Long, Int)]  // (exchangeNodeId, childPlanStageId)
  )

  case class NormalizedNodeDuration(
    nodeId: Long,
    nodeName: String,
    rawMs: Long,
    normalizedMs: Long,
    percentage: Double
  )

  // -----------------------------------------------------------------------------------------
  // Classification — by display name (as shown in SparkPlanGraph)
  // -----------------------------------------------------------------------------------------

  private val BlockingNodes: Set[String] = Set(
    "Sort", "HashAggregate", "ObjectHashAggregate", "SortAggregate",
    "SortMergeJoin", "ShuffledHashJoin", "BroadcastHashJoin",
    "BroadcastNestedLoopJoin", "CartesianProduct",
    // Python UDF nodes have independent timers (each runs its own Python process)
    "BatchEvalPython", "ArrowEvalPython",
    "MapInPandas", "MapInArrow", "PythonMapInArrow",
    "FlatMapGroupsInPandas", "FlatMapCoGroupsInPandas",
    "WindowInPandas", "ArrowWindowPython"
    // Regular Window excluded — pipelined subtraction applies
  )

  private val ExchangeNames: Set[String] = Set(
    "Exchange", "BroadcastExchange",
    "ShuffleQueryStage", "BroadcastQueryStage", "ResultQueryStage"
  )

  private val NativeExclusiveMetrics: Map[String, String] = Map(
    "Sort" -> "sort time",
    "HashAggregate" -> "agg time",
    "ObjectHashAggregate" -> "agg time",
    "ShuffledHashJoin" -> "build time"
  )

  private val TimingMetricNames: Seq[String] = Seq(
    "duration", "sort time", "agg time", "build time"
  )

  // -----------------------------------------------------------------------------------------
  // Stage grouping — purely from graph topology
  // -----------------------------------------------------------------------------------------

  /**
   * Determine stage groups from the graph. Exchange nodes are boundaries.
   * No metric values or Spark stage data needed.
   */
  def computeStageGroups(graph: SparkPlanGraph): Seq[StageGroup] = {
    val allNodes = graph.allNodes.toSeq
    val edges = graph.edges.toSeq

    // Build parent lookup: childId -> parentIds (edges go from child to parent)
    val parentOf = new java.util.HashMap[Long, java.util.ArrayList[Long]]()
    // Build children lookup: parentId -> childIds
    val childrenOf = new java.util.HashMap[Long, java.util.ArrayList[Long]]()

    edges.foreach { edge =>
      parentOf.computeIfAbsent(edge.fromId, _ => new java.util.ArrayList[Long]()).add(edge.toId)
      childrenOf.computeIfAbsent(edge.toId, _ => new java.util.ArrayList[Long]()).add(edge.fromId)
    }

    val nodeById = new java.util.HashMap[Long, SparkPlanGraphNode]()
    allNodes.foreach(n => nodeById.put(n.id, n))

    def isExchange(nodeId: Long): Boolean = {
      val node = nodeById.get(nodeId)
      node != null && isExchangeNode(node.name)
    }

    val visited = new java.util.HashSet[Long]()
    val groups = scala.collection.mutable.ArrayBuffer.empty[StageGroup]
    var stageCounter = 0

    // Find root nodes (no parents — these are output/top nodes in the plan)
    val hasParent = new java.util.HashSet[Long]()
    edges.foreach(e => hasParent.add(e.fromId))
    val roots = allNodes.filter(n => !hasParent.contains(n.id) && !isCodegenCluster(n))

    def collectGroup(startId: Long, planStageId: Int): Unit = {
      val nodeIds = scala.collection.mutable.ArrayBuffer.empty[Long]
      val exchangeIds = scala.collection.mutable.ArrayBuffer.empty[Long]
      val queue = scala.collection.mutable.Queue[Long]()

      // If startId is an exchange (chained exchanges), skip it and seed with its children
      if (isExchange(startId) && !visited.contains(startId)) {
        visited.add(startId)
        exchangeIds += startId
      } else {
        queue.enqueue(startId)
      }

      while (queue.nonEmpty) {
        val id = queue.dequeue()
        if (!visited.contains(id) && !isExchange(id)) {
          visited.add(id)
          val node = nodeById.get(id)
          if (node != null) {
            node match {
              case cluster: SparkPlanGraphCluster =>
                // Codegen cluster: add all inner nodes to the group
                cluster.nodes.foreach { inner =>
                  if (!visited.contains(inner.id)) {
                    visited.add(inner.id)
                    nodeIds += inner.id
                  }
                }
              case _ =>
                nodeIds += id
            }
          }
          // Walk to children (data sources) via graph edges
          val children = childrenOf.get(id)
          if (children != null) {
            val iter = children.iterator()
            while (iter.hasNext) {
              val childId = iter.next()
              if (isExchange(childId)) {
                exchangeIds += childId
              } else if (!visited.contains(childId)) {
                queue.enqueue(childId)
              }
            }
          }
        }
      }

      // Recurse into child stages through exchanges, recording which child group each leads to.
      val exchangeBoundaries = scala.collection.mutable.ArrayBuffer.empty[(Long, Int)]
      exchangeIds.distinct.foreach { exId =>
        visited.add(exId)
        val exChildren = childrenOf.get(exId)
        if (exChildren != null) {
          val iter = exChildren.iterator()
          while (iter.hasNext) {
            val childId = iter.next()
            if (!visited.contains(childId)) {
              stageCounter += 1
              val childStageId = stageCounter
              exchangeBoundaries += ((exId, childStageId))
              collectGroup(childId, childStageId)
            }
          }
        }
      }

      // Always create the group — even if empty (middle stage between chained exchanges)
      groups += StageGroup(planStageId, nodeIds.toSeq, exchangeBoundaries.toSeq)
    }

    roots.foreach { root =>
      if (!visited.contains(root.id)) {
        stageCounter += 1
        collectGroup(root.id, stageCounter)
      }
    }

    groups.toSeq
  }

  /**
   * Resolve plan-stage groups to actual Spark stage IDs using metric values.
   * Returns ResolvedStageGroup with sparkStageId, splitting exchanges into write/read.
   *
   * Stage IDs are extracted from metric value strings containing "(stage X.Y: task Z)".
   * For groups with no metrics, stageId defaults to -1.
   *
   * @param graph the SparkPlanGraph
   * @param metricValues Map[accumulatorId -> value string] from executionMetrics()
   * @param sqlStageIds Set of Spark stage IDs for this SQL execution (from SQLExecutionUIData.stages)
   */
  /**
   * Stage metadata from Spark's stage data.
   * @param stageId Spark stage ID
   * @param status e.g. "COMPLETE", "ACTIVE", "PENDING"
   * @param numTasks total number of tasks
   * @param numCompleteTasks number of completed tasks
   * @param executorRunTime total executor run time in ms
   */
  case class StageInfo(stageId: Int, status: String, numTasks: Int, numCompleteTasks: Int, executorRunTime: Long)

  def resolveStageGroups(
    graph: SparkPlanGraph,
    metricValues: Map[Long, String],
    sqlStageIds: Set[Int],
    stageInfos: Seq[StageInfo] = Seq.empty
  ): Seq[api.ResolvedStageGroup] = {
    val stageGroups = computeStageGroups(graph)
    val allNodes = graph.allNodes.toSeq

    val nodeById = new java.util.HashMap[Long, SparkPlanGraphNode]()
    allNodes.foreach(n => nodeById.put(n.id, n))

    // Extract Spark stageId from a node's metrics by looking for "(stage X.Y: task Z)" pattern
    def extractStageIdFromNode(nodeId: Long): Option[Int] = {
      val node = nodeById.get(nodeId)
      if (node == null) return None
      node.metrics.foreach { metric =>
        metricValues.get(metric.accumulatorId).foreach { value =>
          val stageMatch = """stage\s+(\d+)""".r.findFirstMatchIn(value)
          stageMatch.foreach(m => return Some(m.group(1).toInt))
        }
      }
      None
    }

    // Map planStageId -> sparkStageId using node metrics
    val planToSpark = scala.collection.mutable.Map.empty[Int, Int]
    for (group <- stageGroups) {
      // Try nodes in this group
      val stageId = group.nodeIds.flatMap(extractStageIdFromNode).headOption
      stageId.foreach(id => planToSpark(group.planStageId) = id)
    }

    // Fill gaps using exchange node metrics
    for (group <- stageGroups) {
      for ((exchangeId, childPlanStageId) <- group.exchangeBoundaries) {
        if (!planToSpark.contains(childPlanStageId)) {
          extractStageIdFromNode(exchangeId).foreach { id =>
            // Exchange metrics typically reference the write stage
            planToSpark(childPlanStageId) = id
          }
        }
      }
    }

    // Fill remaining gaps by process of elimination
    val mappedSparkIds = planToSpark.values.toSet
    val unmappedPlanIds = stageGroups.map(_.planStageId).filter(!planToSpark.contains(_)).sorted
    val remainingSparkIds = sqlStageIds.toSeq.filter(!mappedSparkIds.contains(_)).sorted.reverse
    unmappedPlanIds.zip(remainingSparkIds).foreach { case (planId, sparkId) =>
      planToSpark(planId) = sparkId
    }

    // Build stage info lookup
    val stageInfoMap = stageInfos.map(s => s.stageId -> s).toMap

    // Build resolved groups with exchanges split into write/read
    stageGroups.map { group =>
      val sparkStageId = planToSpark.getOrElse(group.planStageId, -1)
      val info = stageInfoMap.get(sparkStageId)

      val exchangeReadIds = group.exchangeBoundaries.map(_._1)
      val exchangeWriteIds = stageGroups.flatMap { parentGroup =>
        parentGroup.exchangeBoundaries.collect {
          case (exId, childId) if childId == group.planStageId => exId
        }
      }

      api.ResolvedStageGroup(
        sparkStageId = sparkStageId,
        status = info.map(_.status).getOrElse("PENDING"),
        numTasks = info.map(_.numTasks).getOrElse(0),
        numCompleteTasks = info.map(_.numCompleteTasks).getOrElse(0),
        executorRunTime = info.map(_.executorRunTime).getOrElse(0L),
        nodeIds = group.nodeIds,
        exchangeWriteIds = exchangeWriteIds,
        exchangeReadIds = exchangeReadIds
      )
    }
  }

  // -----------------------------------------------------------------------------------------
  // Duration attribution
  // -----------------------------------------------------------------------------------------

  /**
   * Compute exclusive durations for all nodes, normalized per-stage to match executorRunTime.
   * Auto-detects instrumentation (looks for "duration" metric on non-codegen nodes).
   *
   * @param graph the SparkPlanGraph
   * @param metricValues Map[accumulatorId -> value string] from SQLAppStatusStore.executionMetrics()
   * @param resolvedGroups resolved stage groups with executorRunTime (for normalization)
   */
  def computeAutoNormalized(
    graph: SparkPlanGraph,
    metricValues: Map[Long, String],
    resolvedGroups: Seq[api.ResolvedStageGroup]
  ): Seq[NodeDuration] = {
    val raw = computeAuto(graph, metricValues)
    val normalized = normalizeByStage(raw, resolvedGroups)

    // Merge exchange durations into the node list (exchanges have writeDurationMs/readDurationMs
    // but those are set via ExchangeDuration and converted to NodeDurationData at the API level)
    val exchangeDurations = computeExchangeDurations(graph, metricValues)
    val exchangeById = exchangeDurations.map(e => e.nodeId -> e).toMap

    // Replace exchange entries in normalized with exchange-specific durations
    normalized.map { nd =>
      exchangeById.get(nd.nodeId) match {
        case Some(ex) => NodeDuration(nd.nodeId, nd.nodeName, ex.writeDurationMs + ex.readDurationMs)
        case None => nd
      }
    }
  }

  /** Compute write/read durations for exchange nodes from their shuffle metrics. */
  def computeExchangeDurations(
    graph: SparkPlanGraph,
    metricValues: Map[Long, String]
  ): Seq[ExchangeDuration] = {
    graph.allNodes.toSeq
      .filter(n => isExchangeNode(n.name))
      .map { node =>
        val writeTime = findMetricValue(node, "shuffle write time", metricValues).getOrElse(0L)
        val fetchWait = findMetricValue(node, "fetch wait time", metricValues).getOrElse(0L)
        val remoteReqs = findMetricValue(node, "remote reqs duration", metricValues).getOrElse(0L)
        val remoteMerged = findMetricValue(node, "remote merged reqs duration", metricValues).getOrElse(0L)
        ExchangeDuration(node.id, writeDurationMs = writeTime, readDurationMs = fetchWait + remoteReqs + remoteMerged)
      }
  }

  /**
   * Compute raw exclusive durations (not normalized).
   */
  def computeAuto(
    graph: SparkPlanGraph,
    metricValues: Map[Long, String]
  ): Seq[NodeDuration] = {
    val hasInstrumentation = graph.allNodes.exists { node =>
      !isCodegenCluster(node) && findMetricValue(node, "duration", metricValues).isDefined
    }
    computeDurations(graph, metricValues, attribution = hasInstrumentation)
  }

  /**
   * Normalize raw durations per stage so they sum to executorRunTime.
   * Nodes with 0 duration stay at 0 — only non-zero nodes are scaled.
   */
  private def normalizeByStage(
    durations: Seq[NodeDuration],
    resolvedGroups: Seq[api.ResolvedStageGroup]
  ): Seq[NodeDuration] = {
    // Build nodeId -> stageGroup lookup
    val nodeToGroup = new java.util.HashMap[Long, api.ResolvedStageGroup]()
    for (group <- resolvedGroups) {
      for (nodeId <- group.nodeIds) {
        nodeToGroup.put(nodeId, group)
      }
    }

    // Build per-stage raw sum of non-zero durations
    val rawSumByStage = new java.util.HashMap[Int, Long]()
    for (d <- durations) {
      if (d.durationMs > 0) {
        val group = nodeToGroup.get(d.nodeId)
        if (group != null) {
          val current = rawSumByStage.getOrDefault(group.sparkStageId, 0L)
          rawSumByStage.put(group.sparkStageId, current + d.durationMs)
        }
      }
    }

    durations.map { d =>
      val group = nodeToGroup.get(d.nodeId)
      if (group == null || d.durationMs <= 0 || group.executorRunTime <= 0) {
        d
      } else {
        val rawSum = rawSumByStage.getOrDefault(group.sparkStageId, 0L)
        if (rawSum <= 0) d
        else {
          val scaleFactor = group.executorRunTime.toDouble / rawSum
          NodeDuration(d.nodeId, d.nodeName, math.round(d.durationMs * scaleFactor))
        }
      }
    }
  }

  /**
   * Compute durations and normalize per stage to executorRunTime.
   */
  def computeAutoNormalized(
    graph: SparkPlanGraph,
    metricValues: Map[Long, String],
    stageGroups: Seq[StageGroup],
    executorRunTimeByPlanStage: Map[Int, Long]
  ): Seq[NormalizedNodeDuration] = {
    val durations = computeAuto(graph, metricValues)
    normalize(durations, stageGroups, executorRunTimeByPlanStage)
  }

  def normalize(
    durations: Seq[NodeDuration],
    stageGroups: Seq[StageGroup],
    executorRunTimeByPlanStage: Map[Int, Long]
  ): Seq[NormalizedNodeDuration] = {
    val totalExecutorRunTime = executorRunTimeByPlanStage.values.sum.toDouble
    val durationByNodeId = durations.map(d => d.nodeId -> d).toMap

    stageGroups.flatMap { group =>
      val stageNodes = group.nodeIds.flatMap(id => durationByNodeId.get(id))
      val rawSum = stageNodes.map(_.durationMs).filter(_ > 0).sum
      val executorRunTime = executorRunTimeByPlanStage.getOrElse(group.planStageId, 0L)

      val scaleFactor =
        if (rawSum > 0 && executorRunTime > 0) executorRunTime.toDouble / rawSum
        else 1.0

      stageNodes.map { nd =>
        val normalizedMs = math.round(nd.durationMs * scaleFactor)
        val percentage =
          if (totalExecutorRunTime > 0) normalizedMs / totalExecutorRunTime * 100.0
          else 0.0
        NormalizedNodeDuration(nd.nodeId, nd.nodeName, nd.durationMs, normalizedMs, percentage)
      }
    }
  }

  // -----------------------------------------------------------------------------------------
  // Internal computation
  // -----------------------------------------------------------------------------------------

  private def computeDurations(
    graph: SparkPlanGraph,
    metricValues: Map[Long, String],
    attribution: Boolean
  ): Seq[NodeDuration] = {
    val allNodes = graph.allNodes.toSeq.filter(!isCodegenCluster(_))
    val edges = graph.edges.toSeq

    // Build children lookup: parentId -> childIds (edges go from child to parent)
    val childrenOf = new java.util.HashMap[Long, java.util.ArrayList[Long]]()
    edges.foreach { edge =>
      childrenOf.computeIfAbsent(edge.toId, _ => new java.util.ArrayList[Long]()).add(edge.fromId)
    }

    val nodeById = new java.util.HashMap[Long, SparkPlanGraphNode]()
    allNodes.foreach(n => nodeById.put(n.id, n))

    // Build inner-node → codegen pipelineTime lookup (fallback for nodes without own timing)
    val codegenDurationByInnerNodeId = new java.util.HashMap[Long, Long]()
    graph.allNodes.toSeq.foreach {
      case cluster: SparkPlanGraphCluster =>
        val pipelineTime = findMetricValue(cluster, "duration", metricValues).getOrElse(0L)
        if (pipelineTime > 0) {
          cluster.nodes.foreach(inner => codegenDurationByInnerNodeId.put(inner.id, pipelineTime))
        }
      case _ =>
    }

    allNodes.map { node =>
      val baseName = extractBaseName(node.name)
      var durationMs = if (attribution) {
        attributeNode(node, baseName, childrenOf, nodeById, metricValues)
      } else {
        classicNode(node, baseName, metricValues)
      }
      // Fallback: if node has NO timing metric and is inside a codegen, use codegen's pipelineTime.
      // Only applies to non-instrumented nodes (like Scan) — nodes with a timing metric that
      // correctly evaluates to 0 (like Project with 0ms exclusive) should stay at 0.
      if (durationMs == 0L && readTimingMs(node, metricValues).isEmpty &&
          nativeExclusiveMs(node, baseName, metricValues).isEmpty) {
        if (codegenDurationByInnerNodeId.containsKey(node.id)) {
          val codegenDuration = codegenDurationByInnerNodeId.get(node.id)
          if (codegenDuration > 0L) {
            durationMs = codegenDuration
          }
        }
      }
      NodeDuration(node.id, node.name, durationMs)
    }
  }

  private def attributeNode(
    node: SparkPlanGraphNode,
    baseName: String,
    childrenOf: java.util.HashMap[Long, java.util.ArrayList[Long]],
    nodeById: java.util.HashMap[Long, SparkPlanGraphNode],
    metricValues: Map[Long, String]
  ): Long = {
    // 1. Native exclusive metric
    nativeExclusiveMs(node, baseName, metricValues).foreach(return _)

    // 2. Read timing
    val selfMs = readTimingMs(node, metricValues).getOrElse(return 0L)

    if (BlockingNodes.contains(baseName)) {
      selfMs
    } else {
      // Pipelined: subtract nearest timed child
      findTimedChild(node.id, childrenOf, nodeById, metricValues) match {
        case Some(childMs) => math.max(0L, selfMs - childMs)
        case None          => selfMs
      }
    }
  }

  private def classicNode(
    node: SparkPlanGraphNode,
    baseName: String,
    metricValues: Map[Long, String]
  ): Long = {
    nativeExclusiveMs(node, baseName, metricValues)
      .orElse(readTimingMs(node, metricValues))
      .getOrElse(0L)
  }

  /** Find the maximum timing value among all descendants reachable without crossing an exchange. */
  private def findTimedChild(
    nodeId: Long,
    childrenOf: java.util.HashMap[Long, java.util.ArrayList[Long]],
    nodeById: java.util.HashMap[Long, SparkPlanGraphNode],
    metricValues: Map[Long, String]
  ): Option[Long] = {
    var maxMs = 0L

    def search(id: Long): Unit = {
      val children = childrenOf.get(id)
      if (children == null) return
      val iter = children.iterator()
      while (iter.hasNext) {
        val childId = iter.next()
        val child = nodeById.get(childId)
        if (child != null && !isExchangeNode(child.name)) {
          readTimingMs(child, metricValues) match {
            case Some(ms) if ms > maxMs => maxMs = ms
            case _ =>
          }
          search(childId) // always search deeper to find the max
        }
      }
    }

    search(nodeId)
    if (maxMs > 0) Some(maxMs) else None
  }

  // -----------------------------------------------------------------------------------------
  // Metric reading helpers
  // -----------------------------------------------------------------------------------------

  /** Read the first timing metric value from a node. */
  private def readTimingMs(node: SparkPlanGraphNode, metricValues: Map[Long, String]): Option[Long] = {
    TimingMetricNames.foreach { name =>
      val value = findMetricValue(node, name, metricValues)
      if (value.isDefined) return value
    }
    None
  }

  /** Read a native exclusive metric if the node type has one. */
  private def nativeExclusiveMs(node: SparkPlanGraphNode, baseName: String, metricValues: Map[Long, String]): Option[Long] = {
    NativeExclusiveMetrics.get(baseName).flatMap { metricName =>
      findMetricValue(node, metricName, metricValues)
    }
  }

  /** Find a metric value by name on a node, joining with metricValues by accumulatorId. */
  private def findMetricValue(node: SparkPlanGraphNode, metricName: String, metricValues: Map[Long, String]): Option[Long] = {
    node.metrics
      .find(_.name.startsWith(metricName))
      .flatMap(metric => metricValues.get(metric.accumulatorId))
      .flatMap(parseMetricValueMs)
  }

  /** Parse a metric value string like "100 ms", "1.5 s", "2.0 m" to milliseconds. */
  private def parseMetricValueMs(value: String): Option[Long] = {
    try {
      // Handle "total (min, med, max)\nVALUE (details...)" format
      val line = if (value.contains("\n")) {
        val parts = value.split("\n")
        if (parts.length >= 2) parts(1).split("\\(")(0).trim else value
      } else value

      val trimmed = line.trim
      if (trimmed.endsWith("ms")) {
        Some(trimmed.dropRight(2).trim.replace(",", "").toDouble.toLong)
      } else if (trimmed.endsWith("s")) {
        Some((trimmed.dropRight(1).trim.replace(",", "").toDouble * 1000).toLong)
      } else if (trimmed.endsWith("m")) {
        Some((trimmed.dropRight(1).trim.replace(",", "").toDouble * 60000).toLong)
      } else if (trimmed.endsWith("h")) {
        Some((trimmed.dropRight(1).trim.replace(",", "").toDouble * 3600000).toLong)
      } else {
        // Try as plain number (milliseconds)
        Some(trimmed.replace(",", "").toDouble.toLong)
      }
    } catch {
      case _: Exception => None
    }
  }

  // -----------------------------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------------------------

  /** Extract base operator name from display name.
   *  "Exchange hashpartitioning(...)" → "Exchange"
   *  "DataFlintSort Sort [...]" → "Sort"
   *  "Sort" → "Sort"
   */
  private def extractBaseName(name: String): String = {
    val stripped = if (name.startsWith("DataFlint")) name.drop(9) else name
    // Take only the first word (the operator name)
    val spaceIdx = stripped.indexOf(' ')
    if (spaceIdx > 0) stripped.substring(0, spaceIdx) else stripped
  }

  private def isExchangeNode(name: String): Boolean =
    ExchangeNames.contains(extractBaseName(name))

  private def isCodegenCluster(node: SparkPlanGraphNode): Boolean =
    node.isInstanceOf[SparkPlanGraphCluster]
}