import { v4 as uuidv4 } from "uuid";
import { EnrichedSparkSQL, EnrichedSqlNode, SQLNodeStageData, SparkJobsStore, SparkStagesStore } from "../interfaces/AppStore";
import { generateGraph } from "./SqlReducer";

export function calculateSQLNodeStage(sql: EnrichedSparkSQL): EnrichedSparkSQL {
    const { nodesIds, edges } = sql.filters["advanced"];

    function findNode(id: number): EnrichedSqlNode {
        return sql.nodes.find(node => node.nodeId === id) as EnrichedSqlNode;
    }

    const nodes = nodesIds.map(id => findNode(id));
    const graph = generateGraph(edges, nodes);

    function findNextNode(id: number): EnrichedSqlNode | undefined {
        const inputEdges = graph.outEdges(id.toString());
        if (inputEdges && inputEdges.length === 1) {
            return findNode(parseInt(inputEdges[0].w));
        }
        return undefined
    }

    function findPreviousNode(id: number): EnrichedSqlNode | undefined {
        const inputEdges = graph.inEdges(id.toString());
        if (inputEdges && inputEdges.length === 1) {
            return findNode(parseInt(inputEdges[0].v));
        }
        return undefined
    }

    const modifiedNodes: EnrichedSqlNode[] = nodes.map(node => {
        if (node.nodeName == "CollectLimit" || node.nodeName === "BroadcastExchange") {
            const previousNode = findPreviousNode(node.nodeId);
            if (previousNode !== undefined && previousNode.stage !== undefined) {
                return { ...node, stage: previousNode.stage };
            }
        }
        return node;
    }).map(node => {
        if (node.nodeName === "AQEShuffleRead") {
            const nextNode = findNextNode(node.nodeId);
            if (nextNode !== undefined && nextNode.stage !== undefined) {
                return { ...node, stage: nextNode.stage };
            }
        }
        return node;
    }).map(node => {
        if (node.nodeName === "Exchange") {
            const nextNode = findNextNode(node.nodeId);
            const previousNode = findPreviousNode(node.nodeId);
            if (previousNode !== undefined && nextNode?.stage !== undefined) {
                const enrichedNode: EnrichedSqlNode = {
                    ...node,
                    stage: {
                        type: "exchange",
                        writeStage: previousNode.stage?.type === "onestage" ? previousNode.stage.stageId : -1,
                        readStage: nextNode.stage?.type === "onestage" ? nextNode.stage.stageId : -1,
                        status: previousNode.stage?.status === "ACTIVE" ? previousNode.stage?.status : nextNode.stage.status
                    }
                };
                return enrichedNode
            }
        }
        return node;
    }).map(node => {
        if (node.type === "input") {
            const nextNode = findNextNode(node.nodeId);
            if (nextNode !== undefined && nextNode.stage !== undefined) {
                return { ...node, stage: nextNode.stage };
            }
        }
        return node;
    }).map(node => {
        if (node.nodeName === "Execute InsertIntoHadoopFsRelationCommand") {
            const previousNode = findPreviousNode(node.nodeId);
            if (previousNode !== undefined && previousNode.stage !== undefined) {
                return { ...node, stage: previousNode.stage };
            }
        }
        return node;
    });
    return { ...sql, nodes: modifiedNodes };
}

export function stageDataFromStage(stageId: number | undefined, stages: SparkStagesStore): SQLNodeStageData | undefined {
    if (stageId === undefined) {
        return undefined;
    }
    const stage = stages.find(stage => stage.stageId === stageId);
    if (stage === undefined) {
        return undefined;
    }
    return {
        type: "onestage",
        stageId: stageId,
        status: stage?.status,
        stageDuration: stage?.metrics.executorRunTime,
    }
}

export function calculateSqlStage(
    sql: EnrichedSparkSQL,
    stages: SparkStagesStore,
    jobs: SparkJobsStore
): EnrichedSparkSQL {
    const allJobsIds = sql.successJobIds
        .concat(sql.failedJobIds)
        .concat(sql.runningJobIds);
    const sqlJobs = jobs.filter((job) => allJobsIds.includes(job.jobId));
    const stagesIds = sqlJobs.flatMap((job) => job.stageIds);
    const sqlStages = stages.filter((stage) => stagesIds.includes(stage.stageId));

    const codegenNodes = sql.codegenNodes.map(node => {
        return { ...node, stage: stageDataFromStage(sqlStages.find(stage => stage.stagesRdd !== undefined && Object.values(stage.stagesRdd).includes(node.nodeName))?.stageId, stages) }
    });
    const nodes = sql.nodes.map(node => {
        const stageCodegen = codegenNodes.find(codegenNode => codegenNode.wholeStageCodegenId === node.wholeStageCodegenId)
        const stageData = stageDataFromStage(stageCodegen?.stage?.stageId, stages);
        const duration = stageCodegen?.codegenDuration ?? node.exchangeMetrics?.duration ?? stageData?.stageDuration;
        const durationPercentage = duration !== undefined && sql.stageMetrics !== undefined ? (sql.stageMetrics?.executorRunTime === 0 ? 0 : ((duration / sql.stageMetrics?.executorRunTime) * 100)) : undefined;
        return {
            ...node,
            stage: stageData,
            duration: duration,
            durationPercentage: durationPercentage
        }
    });
    const knownStageSql = { ...sql, nodes: nodes, codegenNodes: codegenNodes, metricUpdateId: uuidv4() }
    const calculatedStageSql = calculateSQLNodeStage(knownStageSql);
    return calculatedStageSql;
}
