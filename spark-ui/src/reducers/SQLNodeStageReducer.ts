import { v4 as uuidv4 } from "uuid";
import {
  EnrichedSparkSQL,
  EnrichedSqlNode,
  SparkJobsStore,
  SparkStagesStore,
} from "../interfaces/AppStore";
import { calculatePercentage } from "../utils/FormatUtils";
import { calculateNodeToStorageInfo } from "./SqlReducer";
import {
  computeStageGroupsAndDurations,
  ComputedStageGroup,
  NodeDuration,
  StageInfo,
} from "./GraphDurationAttribution";

/**
 * Assign stages to nodes using computed stage groups.
 * Each group has a sparkStageId + node IDs + exchange write/read IDs.
 */
function assignStagesFromGroups(
  nodes: EnrichedSqlNode[],
  stageGroups: ComputedStageGroup[],
): EnrichedSqlNode[] {
  // Build nodeId -> group for regular nodes
  const nodeToGroup = new Map<number, ComputedStageGroup>();
  for (const group of stageGroups) {
    for (const nodeId of group.nodeIds) {
      nodeToGroup.set(nodeId, group);
    }
  }

  // Build exchange write/read stage lookup
  const exchangeWriteStage = new Map<number, { sparkStageId: number; status: string }>();
  const exchangeReadStage = new Map<number, { sparkStageId: number; status: string }>();
  for (const group of stageGroups) {
    for (const exId of group.exchangeWriteIds) {
      exchangeWriteStage.set(exId, { sparkStageId: group.sparkStageId, status: group.status });
    }
    for (const exId of group.exchangeReadIds) {
      exchangeReadStage.set(exId, { sparkStageId: group.sparkStageId, status: group.status });
    }
  }

  return nodes.map((node) => {
    // Exchange node — combine write + read stages
    const write = exchangeWriteStage.get(node.nodeId);
    const read = exchangeReadStage.get(node.nodeId);
    if (write !== undefined || read !== undefined) {
      return {
        ...node,
        stage: {
          type: "exchange" as const,
          writeStage: write?.sparkStageId ?? -1,
          readStage: read?.sparkStageId ?? -1,
          status: write?.status ?? read?.status ?? "PENDING",
        },
      };
    }

    // Regular node — build stage data from computed group
    const group = nodeToGroup.get(node.nodeId);
    if (group === undefined || group.sparkStageId === -1) return node;
    return {
      ...node,
      stage: {
        type: "onestage" as const,
        stageId: group.sparkStageId,
        status: group.status,
        stageDuration: group.executorRunTime,
        restOfStageDuration: undefined,
      },
    };
  });
}

export function calculateSqlStage(
  sql: EnrichedSparkSQL,
  stages: SparkStagesStore,
  jobs: SparkJobsStore,
): EnrichedSparkSQL {
  // Find stages belonging to this SQL
  const allJobsIds = sql.successJobIds
    .concat(sql.failedJobIds)
    .concat(sql.runningJobIds);
  const allJobsIdSet = new Set(allJobsIds);
  const sqlJobs = jobs.filter((job) => allJobsIdSet.has(job.jobId));
  const stagesIdSet = new Set(sqlJobs.flatMap((job) => job.stageIds));
  const sqlStages = stages.filter((stage) => stagesIdSet.has(stage.stageId));

  // Build stage info for the attribution computation
  const sqlStageIds = new Set(sqlStages.map((s) => s.stageId));
  const stageInfos: StageInfo[] = sqlStages.map((s) => ({
    stageId: s.stageId,
    status: s.status,
    numTasks: s.numTasks,
    numCompleteTasks: s.completedTasks,
    executorRunTime: s.metrics.executorRunTime,
  }));

  // Compute stage groups and durations from frontend data
  const { stageGroups, nodeDurations } = computeStageGroupsAndDurations(
    sql.nodes,
    sql.edges,
    sqlStageIds,
    stageInfos,
  );

  // Assign stages to nodes
  const nodesWithStages = assignStagesFromGroups(sql.nodes, stageGroups);

  // Build duration lookup
  const durationMap = new Map<number, NodeDuration>();
  for (const nd of nodeDurations) {
    durationMap.set(nd.nodeId, nd);
  }

  // Assign durations + percentages + exchange write/read durations
  const nodesWithDuration: EnrichedSqlNode[] = nodesWithStages.map((node) => {
    const nd = durationMap.get(node.nodeId);
    const duration = nd !== undefined && nd.durationMs > 0 ? nd.durationMs : undefined;

    const durationPercentage =
      duration !== undefined && sql.stageMetrics !== undefined
        ? calculatePercentage(duration, sql.stageMetrics?.executorRunTime)
        : undefined;

    // For exchange nodes: update write/read durations
    const exchangeMetrics =
      nd?.writeDurationMs || nd?.readDurationMs
        ? {
            ...node.exchangeMetrics,
            writeDuration: nd.writeDurationMs ?? 0,
            readDuration: nd.readDurationMs ?? 0,
            duration: (nd.writeDurationMs ?? 0) + (nd.readDurationMs ?? 0),
          }
        : node.exchangeMetrics;

    return { ...node, duration, durationPercentage, exchangeMetrics };
  });

  // Add storage info
  const nodesToStorageInfo = calculateNodeToStorageInfo(stages, nodesWithDuration);
  const storageInfoByNodeId = new Map<number, (typeof nodesToStorageInfo)[0]["storageInfo"]>();
  for (const item of nodesToStorageInfo) {
    storageInfoByNodeId.set(item.nodeId, item.storageInfo);
  }
  const nodesWithStorageInfo = nodesWithDuration.map((node) => {
    return { ...node, cachedStorage: storageInfoByNodeId.get(node.nodeId) };
  });
  return { ...sql, nodes: nodesWithStorageInfo, metricUpdateId: uuidv4() };
}