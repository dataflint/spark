import { v4 as uuidv4 } from "uuid";
import {
  EnrichedSparkSQL,
  EnrichedSqlNode,
  SparkJobsStore,
  SparkStagesStore,
  SQLNodeStageData,
} from "../interfaces/AppStore";
import { generateGraph } from "./SqlReducer";

export function calculateSQLNodeStage(sql: EnrichedSparkSQL): EnrichedSparkSQL {
  let nodes = sql.nodes;

  function findNode(id: number): EnrichedSqlNode {
    return nodes.find((node) => node.nodeId === id) as EnrichedSqlNode;
  }

  const graph = generateGraph(sql.edges, nodes);

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
    if (node.nodeName === "AQEShuffleRead") {
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
              previousNode.stage?.type === "onestage"
                ? previousNode.stage.stageId
                : -1,
            readStage:
              nextNode.stage?.type === "onestage" ? nextNode.stage.stageId : -1,
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
    if (node.type === "input") {
      const nextNode = findNextNode(node.nodeId);
      if (nextNode !== undefined && nextNode.stage !== undefined) {
        return { ...node, stage: nextNode.stage };
      }
    }
    return node;
  });
  nodes = nodes.map((node) => {
    if (node.nodeName === "Execute InsertIntoHadoopFsRelationCommand") {
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
  return { ...sql, nodes: nodes };
}

export function stageDataFromStage(
  stageId: number | undefined,
  stages: SparkStagesStore,
): SQLNodeStageData | undefined {
  if (stageId === undefined) {
    return undefined;
  }
  const stage = stages.find((stage) => stage.stageId === stageId);
  if (stage === undefined) {
    return undefined;
  }
  return {
    type: "onestage",
    stageId: stageId,
    status: stage?.status,
    stageDuration: stage?.metrics.executorRunTime,
    restOfStageDuration: stage?.metrics.executorRunTime, // will be calculate later
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

  const calculatedStageSql = calculateSQLNodeStage(knownStageSql);

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
          ? sql.stageMetrics?.executorRunTime === 0
            ? 0
            : Math.max(
                0,
                Math.min(100, duration / sql.stageMetrics?.executorRunTime) *
                  100,
              )
          : undefined;
      return {
        ...node,
        duration: duration,
        durationPercentage: durationPercentage,
      };
    },
  );
  return { ...calculatedStageSql, nodes: nodesWithDuration };
}
