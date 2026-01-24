import { v4 as uuidv4 } from "uuid";
import {
  EnrichedSparkSQL,
  EnrichedSqlNode,
  SparkJobsStore,
  SparkStagesStore,
  SQLNodeExchangeStageData,
  SQLNodeStageData,
} from "../interfaces/AppStore";
import { calculatePercentage } from "../utils/FormatUtils";
import { generateGraph } from "./PlanGraphUtils";
import { calculateNodeToStorageInfo } from "./SqlReducer";
import { findExchangeStageIds, isExchangeNode } from "./SqlReducerUtils";

export function calculateSQLNodeStage(sql: EnrichedSparkSQL, sqlStages: SparkStagesStore): EnrichedSparkSQL {
  let nodes = sql.nodes;

  // Build node lookup map for O(1) access
  let nodeById = new Map<number, EnrichedSqlNode>();
  function rebuildNodeMap() {
    nodeById.clear();
    for (const node of nodes) {
      nodeById.set(node.nodeId, node);
    }
  }
  rebuildNodeMap();

  function findNode(id: number): EnrichedSqlNode {
    return nodeById.get(id) as EnrichedSqlNode;
  }

  const graph = generateGraph(sql.edges, nodes);

  function findSingleRootNode(): EnrichedSqlNode | undefined {
    const nodesWithoutInEdges = nodes.filter(node => {
      const inEdges = graph.inEdges(node.nodeId.toString());
      return !inEdges || inEdges.length === 0;
    });

    // if there is only one node without in edges, it is a sql with a single start node
    if (nodesWithoutInEdges.length === 1) {
      return nodesWithoutInEdges[0];
    }
    return undefined;
  }

  function findSingleLastNode(): EnrichedSqlNode | undefined {
    const nodesWithoutOutEdges = nodes.filter(node => {
      const outEdges = graph.outEdges(node.nodeId.toString());
      return !outEdges || outEdges.length === 0;
    });

    // if there is only one node without in edges, it is a sql with a single start node
    if (nodesWithoutOutEdges.length === 1) {
      return nodesWithoutOutEdges[0];
    }
    return undefined;
  }



  function findNextNode(id: number): EnrichedSqlNode | undefined {
    const inputEdges = graph.outEdges(id.toString());
    if (inputEdges && inputEdges.length === 1) {
      return findNode(parseInt(inputEdges[0].w));
    }
    return undefined;
  }

  function findPreviousNode(id: number): EnrichedSqlNode | undefined {
    const inputEdges = graph.inEdges(id.toString());
    if (inputEdges && inputEdges.length === 1) {
      return findNode(parseInt(inputEdges[0].v));
    }
    return undefined;
  }

  const singleRootNode = findSingleRootNode();
  if (singleRootNode !== undefined) {
    const minStageId = Math.min(...sqlStages.map(stage => stage.stageId));
    if (minStageId !== undefined && isFinite(minStageId)) {
      nodes = nodes.map((node) => singleRootNode.nodeId == node.nodeId ?
        { ...node, stage: stageDataFromStage(minStageId, sqlStages) } : node)
    };
  }

  const singleLastNode = findSingleLastNode();
  if (singleLastNode !== undefined) {
    const maxStageId = Math.max(...sqlStages.map(stage => stage.stageId));
    if (maxStageId !== undefined && isFinite(maxStageId)) {
      nodes = nodes.map((node) => singleLastNode.nodeId == node.nodeId ?
        { ...node, stage: stageDataFromStage(maxStageId, sqlStages) } : node)
    };
  }

  rebuildNodeMap(); // Rebuild after initial stage assignments

  nodes = nodes.map((node) => {
    if (
      node.nodeName == "CollectLimit" ||
      node.nodeName === "BroadcastExchange"
    ) {
      const previousNode = findPreviousNode(node.nodeId);
      if (previousNode !== undefined && previousNode.stage !== undefined) {
        return { ...node, stage: previousNode.stage };
      }
    }
    return node;
  });
  nodes = nodes.map((node) => {
    if (node.nodeName === "AQEShuffleRead" || node.nodeName === "Coalesce" ||
      node.nodeName === "BatchEvalPython" || node.nodeName === "MapInPandas" || node.nodeName === "ArrowEvalPython" || node.nodeName === "FlatMapGroupsInPandas") {
      const nextNode = findNextNode(node.nodeId);
      if (nextNode !== undefined && nextNode.stage !== undefined) {
        return { ...node, stage: nextNode.stage };
      }
    }
    return node;
  });
  rebuildNodeMap();
  nodes = nodes.map((node) => {
    // Convert Exchange nodes to exchange stage type if they have adjacent nodes with stage info
    // This handles both nodes without stage data and nodes with onestage type that should be exchange type
    if (node.nodeName === "Exchange" && (node.stage === undefined || node.stage.type === "onestage")) {
      const nextNode = findNextNode(node.nodeId);
      const previousNode = findPreviousNode(node.nodeId);
      const metricsExchangeStageIds = findExchangeStageIds(node.metrics);

      // Get write stage from previous node
      const writeStageId = metricsExchangeStageIds.writeStageId ?? (previousNode?.stage?.type === "onestage"
        ? previousNode.stage.stageId
        : (previousNode?.stage?.type === "exchange" ? previousNode.stage.writeStage : -1));

      // Get read stage from next node
      const readStageId = metricsExchangeStageIds.readStageId ?? (nextNode?.stage?.type === "onestage"
        ? nextNode.stage.stageId
        : (nextNode?.stage?.type === "exchange" ? nextNode.stage.readStage : -1));

      // Convert to exchange type if we have at least the write stage (read stage may not be known yet during execution)
      // or if both stages are known and different
      const hasWriteStage = writeStageId !== -1;
      const hasBothStages = writeStageId !== -1 && readStageId !== -1;
      const shouldBeExchange = hasBothStages && writeStageId !== readStageId;

      if (hasWriteStage && (shouldBeExchange || node.stage === undefined)) {
        const status = previousNode?.stage?.status === "ACTIVE"
          ? previousNode.stage.status
          : (nextNode?.stage?.status ?? previousNode?.stage?.status ?? "PENDING");

        const enrichedNode: EnrichedSqlNode = {
          ...node,
          stage: {
            type: "exchange",
            writeStage: writeStageId,
            readStage: readStageId,
            status: status,
          },
        };
        return enrichedNode;
      }
    }
    return node;
  });
  nodes = nodes.map((node) => {
    if (node.type === "input" && node.stage === undefined) {
      const nextNode = findNextNode(node.nodeId);
      if (nextNode !== undefined && nextNode.stage !== undefined) {
        return { ...node, stage: nextNode.stage };
      }
    }
    return node;
  });
  nodes = nodes.map((node) => {
    if (
      node.nodeName === "Execute InsertIntoHadoopFsRelationCommand" ||
      node.nodeName == "ReplaceData" ||
      node.nodeName == "AppendData"
    ) {
      const previousNode = findPreviousNode(node.nodeId);
      if (previousNode === undefined) {
        return node;
      }
      if (previousNode.stage !== undefined) {
        return { ...node, stage: previousNode.stage };
      }
      // in full graph before insertInto there is a WriteFiles node
      const previousPreviousNode = findPreviousNode(previousNode.nodeId);
      if (
        previousPreviousNode !== undefined &&
        previousPreviousNode.stage !== undefined
      ) {
        return { ...node, stage: previousPreviousNode.stage };
      }
    }
    return node;
  });
  nodes = nodes.map((node) => {
    if (node.nodeName === "Window" && node.stage === undefined) {
      // For Window nodes, try to find stage from next node first, then previous node
      const nextNode = findNextNode(node.nodeId);
      if (nextNode !== undefined && nextNode.stage !== undefined) {
        return { ...node, stage: nextNode.stage };
      }
      const previousNode = findPreviousNode(node.nodeId);
      if (previousNode !== undefined && previousNode.stage !== undefined) {
        return { ...node, stage: previousNode.stage };
      }
    }
    return node;
  });
  nodes = nodes.map((node) => {
    if (node.nodeName === "Union" && node.stage === undefined) {
      const nextNode = findNextNode(node.nodeId);
      if (nextNode !== undefined && nextNode.stage !== undefined) {
        return { ...node, stage: nextNode.stage };
      }
    }
    return node;
  });
  rebuildNodeMap();
  nodes = nodes.map((node) => {
    if (node.stage === undefined) {
      const nextNode = findNextNode(node.nodeId);
      const previousNode = findPreviousNode(node.nodeId);
      if (
        nextNode !== undefined &&
        previousNode !== undefined &&
        nextNode.stage !== undefined &&
        previousNode.stage !== undefined
      ) {
        // Check if both stages are single stages with the same stage ID
        if (
          nextNode.stage.type === "onestage" &&
          previousNode.stage.type === "onestage" &&
          nextNode.stage.stageId === previousNode.stage.stageId
        ) {
          return { ...node, stage: nextNode.stage };
        }
        // Check if next stage is exchange and matches previous stage's write stage
        if (
          nextNode.stage.type === "exchange" &&
          previousNode.stage.type === "onestage" &&
          nextNode.stage.writeStage === previousNode.stage.stageId
        ) {
          return { ...node, stage: previousNode.stage };
        }
        // Check if previous stage is exchange and matches next stage's read stage
        if (
          previousNode.stage.type === "exchange" &&
          nextNode.stage.type === "onestage" &&
          previousNode.stage.readStage === nextNode.stage.stageId
        ) {
          return { ...node, stage: nextNode.stage };
        }
      }
    }
    return node;
  });

  // Propagate stage info from exchange nodes to adjacent nodes without stage data
  rebuildNodeMap();
  nodes = nodes.map((node) => {
    if (node.stage !== undefined) {
      return node;
    }

    // If this node is after an exchange (previousNode is exchange), assign readStage
    const previousNode = findPreviousNode(node.nodeId);
    if (previousNode?.stage?.type === "exchange" && previousNode.stage.readStage !== -1) {
      return { ...node, stage: stageDataFromStage(previousNode.stage.readStage, sqlStages) };
    }

    // If this node is before an exchange (nextNode is exchange), assign writeStage
    const nextNode = findNextNode(node.nodeId);
    if (nextNode?.stage?.type === "exchange" && nextNode.stage.writeStage !== -1) {
      return { ...node, stage: stageDataFromStage(nextNode.stage.writeStage, sqlStages) };
    }

    return node;
  });

  return { ...sql, nodes: nodes };
}

function aggregateStageStatus(stages: SparkStagesStore, stageId: number): string {
  const stageAttempts = stages.filter((stage) => stage.stageId === stageId);
  const statuses = stageAttempts.map(stage => stage.status);
  const uniqueStatuses = Array.from(new Set(statuses));

  // If uniqueStatuses has only one element, return it as the status
  if (uniqueStatuses.length === 1) {
    return uniqueStatuses[0];
  }

  // If any unique status is ACTIVE, then the status is "ACTIVE"
  if (uniqueStatuses.includes("ACTIVE")) {
    return "ACTIVE";
  }

  if (uniqueStatuses.includes("PENDING")) {
    return "PENDING";
  }

  // If it has more than one stage attempt and has success stage, it's "COMPLETE_WITH_RETRIES"
  const hasSuccessStatus = uniqueStatuses.includes("COMPLETE");
  if (stageAttempts.length > 1 && hasSuccessStatus) {
    return "COMPLETE_WITH_RETRIES";
  }

  // Otherwise, sort and join the unique statuses together. Should not happen.
  const sortedUniqueStatuses = uniqueStatuses.sort();
  return sortedUniqueStatuses.join(",");
}

/**
 * Creates exchange stage data from read and write stage IDs.
 * @param readStageId - The read stage ID
 * @param writeStageId - The write stage ID
 * @param stages - Available stages store
 * @returns SQLNodeExchangeStageData or undefined if both stage IDs are missing
 */
function createExchangeStageData(
  readStageId: number | undefined,
  writeStageId: number | undefined,
  stages: SparkStagesStore,
): SQLNodeExchangeStageData | undefined {
  if (readStageId === undefined && writeStageId === undefined) {
    return undefined;
  }

  // Use the available stage ID, preferring readStageId if both are available
  const primaryStageId = readStageId ?? writeStageId;
  const primaryStageStatus = aggregateStageStatus(stages, primaryStageId!);

  return {
    type: "exchange",
    writeStage: writeStageId ?? -1,
    readStage: readStageId ?? -1,
    status: primaryStageStatus,
  };
}

export function stageDataFromStage(
  stageId: number | undefined,
  stages: SparkStagesStore,
): SQLNodeStageData | undefined {
  if (stageId === undefined) {
    return undefined;
  }
  const stageAttempts = stages.filter((stage) => stage.stageId === stageId);
  if (stageAttempts.length === 0) {
    return undefined;
  }

  // Aggregate status using new logic: ACTIVE priority, COMPLETE_WITH_RETRIES for retries, or joined statuses
  const aggregatedStatus = aggregateStageStatus(stages, stageId);

  // Aggregate durations by summing all stage attempts' executorRunTime
  const totalStageDuration = stageAttempts.reduce(
    (sum, stage) => sum + (stage.metrics.executorRunTime || 0),
    0
  );

  return {
    type: "onestage",
    stageId: stageId,
    status: aggregatedStatus,
    stageDuration: totalStageDuration,
    restOfStageDuration: totalStageDuration, // will be calculate later
  };
}

export function calculateSqlStage(
  sql: EnrichedSparkSQL,
  stages: SparkStagesStore,
  jobs: SparkJobsStore,
): EnrichedSparkSQL {
  const allJobsIds = sql.successJobIds
    .concat(sql.failedJobIds)
    .concat(sql.runningJobIds);
  const allJobsIdSet = new Set(allJobsIds);
  const sqlJobs = jobs.filter((job) => allJobsIdSet.has(job.jobId));
  const stagesIdSet = new Set(sqlJobs.flatMap((job) => job.stageIds));
  const sqlStages = stages.filter((stage) => stagesIdSet.has(stage.stageId));

  // Pre-build lookup maps for O(1) access
  const rddValueToStageId = new Map<string, number>();
  const rddKeyToStageId = new Map<string, number>();
  for (const stage of sqlStages) {
    if (stage.stagesRdd !== undefined) {
      for (const [key, value] of Object.entries(stage.stagesRdd)) {
        rddKeyToStageId.set(key, stage.stageId);
        rddValueToStageId.set(value, stage.stageId);
      }
    }
  }

  const codegenNodes = sql.codegenNodes.map((node) => {
    const stageIdByName = rddValueToStageId.get(node.nodeName);
    const stageIdByRddScope = node.rddScopeId !== undefined ? rddKeyToStageId.get(node.rddScopeId) : undefined;
    return {
      ...node,
      stage: stageDataFromStage(stageIdByName ?? stageIdByRddScope, stages),
    };
  });

  // Build codegen lookup map, excluding duplicate codegen IDs
  // If the same codegen ID appears multiple times, we can't reliably determine which stage it belongs to
  const codegenByWholeStageId = new Map<number, typeof codegenNodes[0]>();
  const duplicateCodegenIds = new Set<number>();

  for (const cg of codegenNodes) {
    if (cg.wholeStageCodegenId !== undefined) {
      if (codegenByWholeStageId.has(cg.wholeStageCodegenId)) {
        // This codegen ID appears multiple times, mark as duplicate and remove
        duplicateCodegenIds.add(cg.wholeStageCodegenId);
        codegenByWholeStageId.delete(cg.wholeStageCodegenId);
      } else if (!duplicateCodegenIds.has(cg.wholeStageCodegenId)) {
        // Only add if not already marked as duplicate
        codegenByWholeStageId.set(cg.wholeStageCodegenId, cg);
      }
    }
  }

  const nodes = sql.nodes.map((node) => {
    const databricksRddStageId = node.rddScopeId !== undefined ? rddKeyToStageId.get(node.rddScopeId) : undefined;
    const stageCodegen = node.wholeStageCodegenId !== undefined ? codegenByWholeStageId.get(node.wholeStageCodegenId) : undefined;

    // Handle Exchange nodes separately with special metric-based stage identification
    let stageData: SQLNodeStageData | SQLNodeExchangeStageData | undefined = undefined;
    let metricsStageIdHint: number | undefined = undefined;

    if (databricksRddStageId !== undefined && isExchangeNode(node.nodeName)) {
      // Use exchange-specific metric parsing
      const { readStageId, writeStageId } = findExchangeStageIds(node.metrics);
      stageData = createExchangeStageData(readStageId, writeStageId, stages);
    } else {
      metricsStageIdHint = stageCodegen?.nodeIdFromMetrics ?? node.nodeIdFromMetrics;
    }

    stageData = stageData ?? stageDataFromStage(
      databricksRddStageId ?? stageCodegen?.stage?.stageId ?? metricsStageIdHint,
      stages,
    );
    return {
      ...node,
      stage: stageData,
    };
  });
  const knownStageSql = {
    ...sql,
    nodes: nodes,
    codegenNodes: codegenNodes,
    metricUpdateId: uuidv4(),
  };

  const calculatedStageSql = calculateSQLNodeStage(knownStageSql, sqlStages);

  // Pre-categorize nodes by stage ID for O(1) lookups
  const codegensByStageId = new Map<number, typeof calculatedStageSql.codegenNodes>();
  const exchangeWriteByStageId = new Map<number, typeof calculatedStageSql.nodes>();
  const exchangeReadByStageId = new Map<number, typeof calculatedStageSql.nodes>();
  const broadcastByStageId = new Map<number, typeof calculatedStageSql.nodes>();

  for (const node of calculatedStageSql.codegenNodes) {
    if (node?.stage?.type === "onestage") {
      const arr = codegensByStageId.get(node.stage.stageId) ?? [];
      arr.push(node);
      codegensByStageId.set(node.stage.stageId, arr);
    }
  }
  for (const node of calculatedStageSql.nodes) {
    if (node?.stage?.type === "exchange") {
      const writeArr = exchangeWriteByStageId.get(node.stage.writeStage) ?? [];
      writeArr.push(node);
      exchangeWriteByStageId.set(node.stage.writeStage, writeArr);
      const readArr = exchangeReadByStageId.get(node.stage.readStage) ?? [];
      readArr.push(node);
      exchangeReadByStageId.set(node.stage.readStage, readArr);
    }
    if (node.nodeName === "BroadcastExchange" && node?.stage?.type === "onestage") {
      const arr = broadcastByStageId.get(node.stage.stageId) ?? [];
      arr.push(node);
      broadcastByStageId.set(node.stage.stageId, arr);
    }
  }

  const otherStageDuration = sqlStages.map((stage) => {
    const id = stage.stageId;
    const codegensNodes = codegensByStageId.get(id) ?? [];
    const exchangeWriteNodes = exchangeWriteByStageId.get(id) ?? [];
    const exchangeReadNodes = exchangeReadByStageId.get(id) ?? [];
    const broadcastExchangeNodes = broadcastByStageId.get(id) ?? [];

    const codegensDuration = codegensNodes
      .map((node) => node.codegenDuration ?? 0)
      .reduce((a, b) => a + b, 0);
    const exchangeWriteDuration = exchangeWriteNodes
      .map((node) => node.exchangeMetrics?.writeDuration ?? 0)
      .reduce((a, b) => a + b, 0);
    const exchangeReadDuration = exchangeReadNodes
      .map((node) => node.exchangeMetrics?.readDuration ?? 0)
      .reduce((a, b) => a + b, 0);
    const broadcastExchangeDuration = broadcastExchangeNodes
      .map((node) => node.exchangeBroadcastDuration ?? 0)
      .reduce((a, b) => a + b, 0);

    const restOfStageDuration = Math.max(
      0,
      (stage.metrics.executorRunTime ?? 0) -
      codegensDuration -
      exchangeWriteDuration -
      exchangeReadDuration -
      broadcastExchangeDuration,
    );

    return { id: id, restOfStageDuration: restOfStageDuration };
  });

  // Build lookup map for restOfStageDuration
  const restOfStageDurationMap = new Map<number, number | undefined>();
  for (const item of otherStageDuration) {
    restOfStageDurationMap.set(item.id, item.restOfStageDuration);
  }

  const nodesWithStageDuration: EnrichedSqlNode[] =
    calculatedStageSql.nodes.map((node) => {
      if (
        node.exchangeMetrics !== undefined ||
        node.wholeStageCodegenId !== undefined
      ) {
        return node;
      }
      if (node.stage === undefined) {
        return node;
      }
      const restOfStageDuration = node.stage?.type === "onestage"
        ? restOfStageDurationMap.get(node.stage.stageId)
        : undefined;
      return {
        ...node,
        stage: { ...node.stage, restOfStageDuration: restOfStageDuration },
      };
    });

  const nodesWithDuration: EnrichedSqlNode[] = nodesWithStageDuration.map(
    (node) => {
      const stageCodegen = node.wholeStageCodegenId !== undefined
        ? codegenByWholeStageId.get(node.wholeStageCodegenId)
        : undefined;
      const duration =
        stageCodegen?.codegenDuration ??
        node.exchangeMetrics?.duration ??
        (node.stage?.type === "onestage"
          ? node.stage?.restOfStageDuration ?? node.stage?.stageDuration
          : undefined);

      // Cap duration by stage duration for onestage nodes to prevent invalid values.
      // Sometimes in databricks the codeged duration is greated than the stage duration.
      const cappedDuration = duration !== undefined && node.stage?.type === "onestage"
        ? Math.min(duration, node.stage?.stageDuration ?? duration)
        : duration;
      const durationPercentage =
        cappedDuration !== undefined && sql.stageMetrics !== undefined
          ? calculatePercentage(cappedDuration, sql.stageMetrics?.executorRunTime) : undefined
      return {
        ...node,
        duration: cappedDuration,
        durationPercentage: durationPercentage,
      };
    },
  );

  const nodesToStorageInfo = calculateNodeToStorageInfo(stages, nodesWithDuration);
  // Build lookup map for storage info
  const storageInfoByNodeId = new Map<number, typeof nodesToStorageInfo[0]["storageInfo"]>();
  for (const item of nodesToStorageInfo) {
    storageInfoByNodeId.set(item.nodeId, item.storageInfo);
  }
  const nodesWithStorageInfo = nodesWithDuration.map((node) => {
    return { ...node, cachedStorage: storageInfoByNodeId.get(node.nodeId) };
  });
  return { ...calculatedStageSql, nodes: nodesWithStorageInfo };
}
