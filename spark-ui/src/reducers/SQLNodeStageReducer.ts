import { v4 as uuidv4 } from "uuid";
import {
  EnrichedSparkSQL,
  EnrichedSqlNode,
  SparkJobsStore,
  SparkStagesStore,
  SQLNodeStageData,
} from "../interfaces/AppStore";
import { calculatePercentage } from "../utils/FormatUtils";
import { generateGraph } from "./PlanGraphUtils";
import { calculateNodeToStorageInfo } from "./SqlReducer";

export function calculateSQLNodeStage(sql: EnrichedSparkSQL, sqlStages: SparkStagesStore): EnrichedSparkSQL {
  let nodes = sql.nodes;

  function findNode(id: number): EnrichedSqlNode {
    return nodes.find((node) => node.nodeId === id) as EnrichedSqlNode;
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
  nodes = nodes.map((node) => {
    if (node.nodeName === "Exchange") {
      const nextNode = findNextNode(node.nodeId);
      const previousNode = findPreviousNode(node.nodeId);
      if (previousNode !== undefined && nextNode?.stage !== undefined) {
        const enrichedNode: EnrichedSqlNode = {
          ...node,
          stage: {
            type: "exchange",
            writeStage:
              nextNode.stage?.type === "onestage" ? nextNode.stage.stageId : -1,
            readStage:
              previousNode.stage?.type === "onestage"
                ? previousNode.stage.stageId
                : -1,
            status:
              previousNode.stage?.status === "ACTIVE"
                ? previousNode.stage?.status
                : nextNode.stage.status,
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
  const sqlJobs = jobs.filter((job) => allJobsIds.includes(job.jobId));
  const stagesIds = sqlJobs.flatMap((job) => job.stageIds);
  const sqlStages = stages.filter((stage) => stagesIds.includes(stage.stageId));

  const codegenNodes = sql.codegenNodes.map((node) => {
    return {
      ...node,
      stage: stageDataFromStage(
        sqlStages.find(
          (stage) =>
            stage.stagesRdd !== undefined &&
            (Object.values(stage.stagesRdd).includes(node.nodeName) ||
              (node.rddScopeId !== undefined &&
                Object.keys(stage.stagesRdd).includes(node.rddScopeId))),
        )?.stageId,
        stages,
      ),
    };
  });
  const nodes = sql.nodes.map((node) => {
    const databricksRddStageId = sqlStages.find(
      (stage) =>
        stage.stagesRdd !== undefined &&
        node.rddScopeId !== undefined &&
        Object.keys(stage.stagesRdd).includes(node.rddScopeId),
    )?.stageId;
    const stageCodegen = codegenNodes.find(
      (codegenNode) =>
        codegenNode.wholeStageCodegenId === node.wholeStageCodegenId,
    );
    const stageData = stageDataFromStage(
      databricksRddStageId ?? stageCodegen?.stage?.stageId,
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

  const otherStageDuration = sqlStages.map((stage) => {
    const id = stage.stageId;
    const codegensNodes = calculatedStageSql.codegenNodes.filter(
      (node) => node?.stage?.type === "onestage" && node?.stage?.stageId === id,
    );
    const exchangeWriteNodes = calculatedStageSql.nodes.filter(
      (node) =>
        node?.stage?.type === "exchange" && node?.stage?.writeStage === id,
    );
    const exchangeReadNodes = calculatedStageSql.nodes.filter(
      (node) =>
        node?.stage?.type === "exchange" && node?.stage?.readStage === id,
    );
    const broadcastExchangeNodes = calculatedStageSql.nodes.filter(
      (node) =>
        node.nodeName === "BroadcastExchange" &&
        node?.stage?.type === "onestage" &&
        node?.stage?.stageId === id,
    );

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
      const restOfStageDuration = otherStageDuration.find(
        (stage) =>
          node.stage?.type === "onestage" && stage.id === node.stage?.stageId,
      )?.restOfStageDuration;
      return {
        ...node,
        stage: { ...node.stage, restOfStageDuration: restOfStageDuration },
      };
    });

  const nodesWithDuration: EnrichedSqlNode[] = nodesWithStageDuration.map(
    (node) => {
      const stageCodegen = codegenNodes.find(
        (codegenNode) =>
          codegenNode.wholeStageCodegenId === node.wholeStageCodegenId,
      );
      const duration =
        stageCodegen?.codegenDuration ??
        node.exchangeMetrics?.duration ??
        (node.stage?.type === "onestage"
          ? node.stage?.restOfStageDuration ?? node.stage?.stageDuration
          : undefined);
      const durationPercentage =
        duration !== undefined && sql.stageMetrics !== undefined
          ? calculatePercentage(duration, sql.stageMetrics?.executorRunTime) : undefined
      return {
        ...node,
        duration: duration,
        durationPercentage: durationPercentage,
      };
    },
  );

  const nodesToStorageInfo = calculateNodeToStorageInfo(stages, nodesWithDuration);
  const nodesWithStorageInfo = nodesWithDuration.map((node) => {
    const stageToStorageInfo = nodesToStorageInfo.find(
      (nodeToStorageInfo) => nodeToStorageInfo.nodeId === node.nodeId,
    );
    return { ...node, cachedStorage: stageToStorageInfo?.storageInfo };
  });
  return { ...calculatedStageSql, nodes: nodesWithStorageInfo };
}
