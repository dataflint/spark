import { v4 as uuidv4 } from "uuid";
import {
  EnrichedSparkSQL,
  EnrichedSqlNode,
  NodeDurationData,
  SparkJobsStore,
  SparkStagesStore,
  ResolvedStageGroup,
} from "../interfaces/AppStore";
import { calculatePercentage } from "../utils/FormatUtils";
import { calculateNodeToStorageInfo } from "./SqlReducer";

/**
 * Assign stages to nodes using backend-resolved stage groups.
 * Each group has a sparkStageId + node IDs + exchange write/read IDs.
 * No guessing or mapping needed — the backend resolved everything.
 */
function assignStagesFromBackendGroups(
  nodes: EnrichedSqlNode[],
  stageGroups: ResolvedStageGroup[],
): EnrichedSqlNode[] {
  // Build nodeId -> group for regular nodes
  const nodeToGroup = new Map<number, ResolvedStageGroup>();
  for (const group of stageGroups) {
    for (const nodeId of group.nodeIds) {
      nodeToGroup.set(nodeId, group);
    }
  }

  // Build exchange write/read stage lookup
  const exchangeWriteStage = new Map<number, { sparkStageId: number, status: string }>();
  const exchangeReadStage = new Map<number, { sparkStageId: number, status: string }>();
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

    // Regular node — build stage data directly from backend group
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

export function calculateSQLNodeStage(sql: EnrichedSparkSQL): EnrichedSparkSQL {
  if (sql.backendStageGroups && sql.backendStageGroups.length > 0) {
    const assignedNodes = assignStagesFromBackendGroups(sql.nodes, sql.backendStageGroups);
    return { ...sql, nodes: assignedNodes };
  }
  return sql;
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

  // Assign stages using backend-resolved stage groups
  const calculatedStageSql = calculateSQLNodeStage(
    { ...sql, metricUpdateId: uuidv4() },
  );

  // Backend-computed durations from GraphDurationAttribution
  const backendDurationMap = new Map<number, NodeDurationData>();
  if (calculatedStageSql.backendNodeDurations) {
    for (const nd of calculatedStageSql.backendNodeDurations) {
      backendDurationMap.set(nd.nodeId, nd);
    }
  }

  // Assign durations + percentages + exchange write/read durations
  const nodesWithDuration: EnrichedSqlNode[] = calculatedStageSql.nodes.map(
    (node) => {
      const backendData = backendDurationMap.get(node.nodeId);
      const duration = backendData !== undefined && backendData.durationMs > 0 ? backendData.durationMs : undefined;

      const durationPercentage =
        duration !== undefined && sql.stageMetrics !== undefined
          ? calculatePercentage(duration, sql.stageMetrics?.executorRunTime) : undefined;

      // For exchange nodes: update write/read durations from backend
      const exchangeMetrics = (backendData?.writeDurationMs || backendData?.readDurationMs)
        ? {
            ...node.exchangeMetrics,
            writeDuration: backendData.writeDurationMs ?? 0,
            readDuration: backendData.readDurationMs ?? 0,
            duration: (backendData.writeDurationMs ?? 0) + (backendData.readDurationMs ?? 0),
          }
        : node.exchangeMetrics;

      return { ...node, duration, durationPercentage, exchangeMetrics };
    },
  );

  // Add storage info
  const nodesToStorageInfo = calculateNodeToStorageInfo(stages, nodesWithDuration);
  const storageInfoByNodeId = new Map<number, typeof nodesToStorageInfo[0]["storageInfo"]>();
  for (const item of nodesToStorageInfo) {
    storageInfoByNodeId.set(item.nodeId, item.storageInfo);
  }
  const nodesWithStorageInfo = nodesWithDuration.map((node) => {
    return { ...node, cachedStorage: storageInfoByNodeId.get(node.nodeId) };
  });
  return { ...calculatedStageSql, nodes: nodesWithStorageInfo };
}