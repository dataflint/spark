import { EnrichedSqlEdge, EnrichedSqlMetric, EnrichedSqlNode } from "../interfaces/AppStore";
import { getMetricDuration } from "./SqlReducer";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface StageGroup {
  planStageId: number;
  nodeIds: number[];
  exchangeBoundaries: Array<{ exchangeNodeId: number; childPlanStageId: number }>;
}

export interface ComputedStageGroup {
  sparkStageId: number;
  status: string;
  numTasks: number;
  numCompleteTasks: number;
  executorRunTime: number;
  nodeIds: number[];
  exchangeWriteIds: number[];
  exchangeReadIds: number[];
}

export interface NodeDuration {
  nodeId: number;
  nodeName: string;
  durationMs: number;
  writeDurationMs?: number;
  readDurationMs?: number;
}

export interface StageInfo {
  stageId: number;
  status: string;
  numTasks: number;
  numCompleteTasks: number;
  executorRunTime: number;
}

// ---------------------------------------------------------------------------
// Constants — must match Scala exactly
// ---------------------------------------------------------------------------

const BLOCKING_NODES = new Set([
  "Sort", "HashAggregate", "ObjectHashAggregate", "SortAggregate",
  "SortMergeJoin", "ShuffledHashJoin", "BroadcastHashJoin",
  "BroadcastNestedLoopJoin", "CartesianProduct",
  "BatchEvalPython", "ArrowEvalPython",
  "MapInPandas", "MapInArrow", "PythonMapInArrow",
  "FlatMapGroupsInPandas", "FlatMapCoGroupsInPandas",
  "WindowInPandas", "ArrowWindowPython",
]);

const EXCHANGE_NAMES = new Set([
  "Exchange", "BroadcastExchange",
  "ShuffleQueryStage", "BroadcastQueryStage", "ResultQueryStage",
]);

const NATIVE_EXCLUSIVE_METRICS: Record<string, string> = {
  "Sort": "sort time",
  "HashAggregate": "agg time",
  "ObjectHashAggregate": "agg time",
  "ShuffledHashJoin": "build time",
};

const TIMING_METRIC_NAMES = ["duration", "sort time", "agg time", "build time"];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function extractBaseName(name: string): string {
  const spaceIdx = name.indexOf(" ");
  return spaceIdx > 0 ? name.substring(0, spaceIdx) : name;
}

function isExchangeNode(name: string): boolean {
  return EXCHANGE_NAMES.has(extractBaseName(name));
}

// ---------------------------------------------------------------------------
// 1. Stage grouping — purely from graph topology
// ---------------------------------------------------------------------------

export function computeStageGroups(
  nodes: EnrichedSqlNode[],
  edges: EnrichedSqlEdge[],
): StageGroup[] {
  // Build children lookup: parentId -> childIds
  // edges go from child (fromId) to parent (toId)
  const childrenOf = new Map<number, number[]>();
  for (const edge of edges) {
    let children = childrenOf.get(edge.toId);
    if (children === undefined) {
      children = [];
      childrenOf.set(edge.toId, children);
    }
    children.push(edge.fromId);
  }

  const nodeById = new Map<number, EnrichedSqlNode>();
  for (const n of nodes) {
    nodeById.set(n.nodeId, n);
  }

  function isExchange(nodeId: number): boolean {
    const node = nodeById.get(nodeId);
    return node !== undefined && isExchangeNode(node.nodeName);
  }

  const visited = new Set<number>();
  const groups: StageGroup[] = [];
  let stageCounter = 0;

  // Find root nodes: nodes that have NO edge with fromId === node.nodeId (no parent)
  // AND are not codegen cluster nodes
  const hasParent = new Set<number>();
  for (const e of edges) {
    hasParent.add(e.fromId);
  }
  const roots = nodes.filter(n => !hasParent.has(n.nodeId) && !n.isCodegenNode);

  function collectGroup(startId: number, planStageId: number): void {
    const nodeIds: number[] = [];
    const exchangeIds: number[] = [];
    const queue: number[] = [];

    // If startId is an exchange (chained exchanges), skip it and seed with its children
    if (isExchange(startId) && !visited.has(startId)) {
      visited.add(startId);
      exchangeIds.push(startId);
    } else {
      queue.push(startId);
    }

    while (queue.length > 0) {
      const id = queue.shift()!;
      if (!visited.has(id) && !isExchange(id)) {
        visited.add(id);
        const node = nodeById.get(id);
        if (node !== undefined) {
          if (node.isCodegenNode) {
            // Codegen cluster: add all other nodes with same wholeStageCodegenId
            for (const inner of nodes) {
              if (
                !inner.isCodegenNode &&
                inner.wholeStageCodegenId !== undefined &&
                inner.wholeStageCodegenId === node.wholeStageCodegenId &&
                !visited.has(inner.nodeId)
              ) {
                visited.add(inner.nodeId);
                nodeIds.push(inner.nodeId);
              }
            }
          } else {
            nodeIds.push(id);
          }
        }
        // Walk to children via graph edges
        const children = childrenOf.get(id);
        if (children !== undefined) {
          for (const childId of children) {
            if (isExchange(childId)) {
              exchangeIds.push(childId);
            } else if (!visited.has(childId)) {
              queue.push(childId);
            }
          }
        }
      }
    }

    // Recurse into child stages through exchanges
    const exchangeBoundaries: Array<{ exchangeNodeId: number; childPlanStageId: number }> = [];
    const distinctExchangeIds = Array.from(new Set(exchangeIds));
    for (const exId of distinctExchangeIds) {
      visited.add(exId);
      const exChildren = childrenOf.get(exId);
      if (exChildren !== undefined) {
        for (const childId of exChildren) {
          if (!visited.has(childId)) {
            stageCounter += 1;
            const childStageId = stageCounter;
            exchangeBoundaries.push({ exchangeNodeId: exId, childPlanStageId: childStageId });
            collectGroup(childId, childStageId);
          }
        }
      }
    }

    // Always create the group — even if empty
    groups.push({ planStageId, nodeIds, exchangeBoundaries });
  }

  for (const root of roots) {
    if (!visited.has(root.nodeId)) {
      stageCounter += 1;
      collectGroup(root.nodeId, stageCounter);
    }
  }

  return groups;
}

// ---------------------------------------------------------------------------
// 2. Resolve stage groups to Spark stage IDs
// ---------------------------------------------------------------------------

export function resolveStageGroups(
  stageGroups: StageGroup[],
  nodes: EnrichedSqlNode[],
  sqlStageIds: Set<number>,
  stageInfos: StageInfo[],
): ComputedStageGroup[] {
  const nodeById = new Map<number, EnrichedSqlNode>();
  for (const n of nodes) {
    nodeById.set(n.nodeId, n);
  }

  // Phase 1: extract stage ID from node metrics (nodeIdFromMetrics)
  const planToSpark = new Map<number, number>();
  for (const group of stageGroups) {
    for (const nid of group.nodeIds) {
      const node = nodeById.get(nid);
      if (node !== undefined && node.nodeIdFromMetrics !== undefined) {
        planToSpark.set(group.planStageId, node.nodeIdFromMetrics);
        break;
      }
    }
  }

  // Phase 2: fill gaps using exchange boundary nodes
  for (const group of stageGroups) {
    for (const boundary of group.exchangeBoundaries) {
      if (!planToSpark.has(boundary.childPlanStageId)) {
        const exNode = nodeById.get(boundary.exchangeNodeId);
        if (exNode !== undefined && exNode.nodeIdFromMetrics !== undefined) {
          planToSpark.set(boundary.childPlanStageId, exNode.nodeIdFromMetrics);
        }
      }
    }
  }

  // Phase 3: process of elimination
  const mappedSparkIds = new Set(Array.from(planToSpark.values()));
  const unmappedPlanIds = stageGroups
    .map(g => g.planStageId)
    .filter(id => !planToSpark.has(id))
    .sort((a, b) => a - b);
  const remainingSparkIds = Array.from(sqlStageIds)
    .filter(id => !mappedSparkIds.has(id))
    .sort((a, b) => b - a); // reverse sort

  for (let i = 0; i < Math.min(unmappedPlanIds.length, remainingSparkIds.length); i++) {
    planToSpark.set(unmappedPlanIds[i], remainingSparkIds[i]);
  }

  // Build stage info lookup
  const stageInfoMap = new Map<number, StageInfo>();
  for (const s of stageInfos) {
    stageInfoMap.set(s.stageId, s);
  }

  // Build resolved groups with exchanges split into write/read
  return stageGroups.map(group => {
    const sparkStageId = planToSpark.get(group.planStageId) ?? -1;
    const info = stageInfoMap.get(sparkStageId);

    const exchangeReadIds = group.exchangeBoundaries.map(b => b.exchangeNodeId);
    const exchangeWriteIds = stageGroups.flatMap(parentGroup =>
      parentGroup.exchangeBoundaries
        .filter(b => b.childPlanStageId === group.planStageId)
        .map(b => b.exchangeNodeId),
    );

    return {
      sparkStageId,
      status: info?.status ?? "PENDING",
      numTasks: info?.numTasks ?? 0,
      numCompleteTasks: info?.numCompleteTasks ?? 0,
      executorRunTime: info?.executorRunTime ?? 0,
      nodeIds: group.nodeIds,
      exchangeWriteIds,
      exchangeReadIds,
    };
  });
}

// ---------------------------------------------------------------------------
// 3. Duration attribution
// ---------------------------------------------------------------------------

/** Read the first timing metric value from a node. */
function readTimingMs(metrics: EnrichedSqlMetric[]): number | undefined {
  for (const name of TIMING_METRIC_NAMES) {
    const value = getMetricDuration(name, metrics);
    if (value !== undefined) return value;
  }
  return undefined;
}

/** Read a native exclusive metric if the node type has one. */
function nativeExclusiveMs(baseName: string, metrics: EnrichedSqlMetric[]): number | undefined {
  const metricName = NATIVE_EXCLUSIVE_METRICS[baseName];
  if (metricName === undefined) return undefined;
  return getMetricDuration(metricName, metrics);
}

/**
 * Find the maximum timing value among all descendants reachable without
 * crossing an exchange or blocking node.
 */
function findTimedChild(
  nodeId: number,
  childrenOf: Map<number, number[]>,
  nodeById: Map<number, EnrichedSqlNode>,
): number | undefined {
  let maxMs = 0;

  function search(id: number): void {
    const children = childrenOf.get(id);
    if (children === undefined) return;
    for (const childId of children) {
      const child = nodeById.get(childId);
      if (child !== undefined && !isExchangeNode(child.nodeName)) {
        const baseName = extractBaseName(child.nodeName);
        if (!BLOCKING_NODES.has(baseName)) {
          const ms = readTimingMs(child.metrics);
          if (ms !== undefined && ms > maxMs) {
            maxMs = ms;
          }
          search(childId); // always search deeper to find the max
        }
      }
    }
  }

  search(nodeId);
  return maxMs > 0 ? maxMs : undefined;
}

function attributeNode(
  node: EnrichedSqlNode,
  baseName: string,
  childrenOf: Map<number, number[]>,
  nodeById: Map<number, EnrichedSqlNode>,
): number {
  // 1. Native exclusive metric
  const nativeMs = nativeExclusiveMs(baseName, node.metrics);
  if (nativeMs !== undefined) return nativeMs;

  // 2. Read timing
  const selfMs = readTimingMs(node.metrics);
  if (selfMs === undefined) return 0;

  if (BLOCKING_NODES.has(baseName)) {
    return selfMs;
  }

  // Pipelined: subtract nearest timed child
  const childMs = findTimedChild(node.nodeId, childrenOf, nodeById);
  if (childMs !== undefined) {
    return Math.max(0, selfMs - childMs);
  }
  return selfMs;
}

function classicNode(
  node: EnrichedSqlNode,
  baseName: string,
): number {
  return nativeExclusiveMs(baseName, node.metrics)
    ?? readTimingMs(node.metrics)
    ?? 0;
}

export function computeDurations(
  nodes: EnrichedSqlNode[],
  edges: EnrichedSqlEdge[],
  codegenNodes: EnrichedSqlNode[] = [],
  mode: "exclusive" | "inclusive" = "exclusive",
): NodeDuration[] {
  // In inclusive mode, always use classicNode (raw metric values, no subtraction)
  const hasInstrumentation = mode === "inclusive" ? false : nodes.some(
    n => !n.isCodegenNode && getMetricDuration("duration", n.metrics) !== undefined,
  );

  const nonCodegenNodes = nodes.filter(n => !n.isCodegenNode);

  // Build children lookup: parentId -> childIds
  const childrenOf = new Map<number, number[]>();
  for (const edge of edges) {
    let children = childrenOf.get(edge.toId);
    if (children === undefined) {
      children = [];
      childrenOf.set(edge.toId, children);
    }
    children.push(edge.fromId);
  }

  const nodeById = new Map<number, EnrichedSqlNode>();
  for (const n of nonCodegenNodes) {
    nodeById.set(n.nodeId, n);
  }

  return nonCodegenNodes.map(node => {
    const baseName = extractBaseName(node.nodeName);

    let durationMs: number;
    if (hasInstrumentation) {
      durationMs = attributeNode(node, baseName, childrenOf, nodeById);
    } else {
      durationMs = classicNode(node, baseName);
    }

    // Codegen fallback: if durationMs === 0 AND no timing metric found
    // AND no native exclusive metric, use the codegen node's codegenDuration
    if (
      durationMs === 0 &&
      readTimingMs(node.metrics) === undefined &&
      nativeExclusiveMs(baseName, node.metrics) === undefined
    ) {
      if (node.wholeStageCodegenId !== undefined) {
        // Find the codegen node with matching wholeStageCodegenId (codegen nodes are in a separate array)
        const codegenNode = codegenNodes.find(
          n => n.wholeStageCodegenId === node.wholeStageCodegenId,
        );
        if (codegenNode !== undefined && codegenNode.codegenDuration !== undefined && codegenNode.codegenDuration > 0) {
          durationMs = codegenNode.codegenDuration;
        }
      }
    }

    return { nodeId: node.nodeId, nodeName: node.nodeName, durationMs };
  });
}

// ---------------------------------------------------------------------------
// 4. Normalize by stage
// ---------------------------------------------------------------------------

export function normalizeByStage(
  durations: NodeDuration[],
  resolvedGroups: ComputedStageGroup[],
): NodeDuration[] {
  // Build nodeId -> group lookup
  const nodeToGroup = new Map<number, ComputedStageGroup>();
  for (const group of resolvedGroups) {
    for (const nodeId of group.nodeIds) {
      nodeToGroup.set(nodeId, group);
    }
  }

  // Build per-stage raw sum of non-zero durations
  const rawSumByStage = new Map<number, number>();
  for (const d of durations) {
    if (d.durationMs > 0) {
      const group = nodeToGroup.get(d.nodeId);
      if (group !== undefined) {
        const current = rawSumByStage.get(group.sparkStageId) ?? 0;
        rawSumByStage.set(group.sparkStageId, current + d.durationMs);
      }
    }
  }

  return durations.map(d => {
    const group = nodeToGroup.get(d.nodeId);
    if (group === undefined || d.durationMs <= 0 || group.executorRunTime <= 0) {
      return d;
    }
    const rawSum = rawSumByStage.get(group.sparkStageId) ?? 0;
    if (rawSum <= 0) return d;

    const normalizedMs = Math.round(d.durationMs * group.executorRunTime / rawSum);
    return { ...d, durationMs: normalizedMs };
  });
}

// ---------------------------------------------------------------------------
// 5. Exchange durations
// ---------------------------------------------------------------------------

export function computeExchangeDurations(
  nodes: EnrichedSqlNode[],
): NodeDuration[] {
  return nodes
    .filter(n => isExchangeNode(n.nodeName) && n.exchangeMetrics !== undefined)
    .map(n => {
      const ex = n.exchangeMetrics!;
      return {
        nodeId: n.nodeId,
        nodeName: n.nodeName,
        durationMs: ex.writeDuration + ex.readDuration,
        writeDurationMs: ex.writeDuration,
        readDurationMs: ex.readDuration,
      };
    });
}

// ---------------------------------------------------------------------------
// 6. Main export — orchestrator
// ---------------------------------------------------------------------------

export function computeStageGroupsAndDurations(
  nodes: EnrichedSqlNode[],
  edges: EnrichedSqlEdge[],
  sqlStageIds: Set<number>,
  stageInfos: StageInfo[],
  codegenNodes: EnrichedSqlNode[] = [],
  mode: "exclusive" | "inclusive" = "exclusive",
): { stageGroups: ComputedStageGroup[]; nodeDurations: NodeDuration[] } {
  // Step 1: Compute stage groups from topology
  const rawStageGroups = computeStageGroups(nodes, edges);

  // Step 2: Resolve plan stage IDs to Spark stage IDs
  const resolvedGroups = resolveStageGroups(rawStageGroups, nodes, sqlStageIds, stageInfos);

  // Step 3: Compute raw durations
  const rawDurations = computeDurations(nodes, edges, codegenNodes, mode);

  // Step 4: Compute exchange durations (same in both modes)
  const exchangeDurations = computeExchangeDurations(nodes);
  const exchangeById = new Map<number, NodeDuration>();
  for (const ex of exchangeDurations) {
    exchangeById.set(ex.nodeId, ex);
  }

  if (mode === "inclusive") {
    // Inclusive: raw durations as-is, no normalization, no budget
    const mergedRaw = rawDurations.map(nd => exchangeById.get(nd.nodeId) ?? nd);
    return { stageGroups: resolvedGroups, nodeDurations: mergedRaw };
  }

  // Exclusive mode: normalize by stage executorRunTime
  const normalizedDurations = normalizeByStage(rawDurations, resolvedGroups);

  // Merge exchange durations into normalized list
  const mergedDurations = normalizedDurations.map(nd => exchangeById.get(nd.nodeId) ?? nd);

  return { stageGroups: resolvedGroups, nodeDurations: mergedDurations };
}