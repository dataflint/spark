import { ApiAction } from "../interfaces/APIAction";
import { AppStore, EnrichedSparkSQL, EnrichedSqlMetric, NodeType, SparkSQLStore, StatusStore, EnrichedSqlEdge, EnrichedSqlNode } from '../interfaces/AppStore';
import { SparkConfiguration } from "../interfaces/SparkConfiguration";
import { SparkSQL, SparkSQLs } from "../interfaces/SparkSQLs";
import { SparkStages } from "../interfaces/SparkStages";
import { humanFileSize } from "../utils/FormatUtils";
import isEqual from 'lodash/isEqual';
import { Edge, Graph } from 'graphlib';

function extractConfig(sparkConfiguration: SparkConfiguration): [string, Record<string, string>] {
    const sparkPropertiesObj = Object.fromEntries(sparkConfiguration.sparkProperties);
    const systemPropertiesObj = Object.fromEntries(sparkConfiguration.systemProperties);
    const runtimeObj = sparkConfiguration.runtime;

    const appName = sparkPropertiesObj["spark.app.name"];
    const config = {
        "spark.app.name": sparkPropertiesObj["spark.app.name"],
        "spark.app.id": sparkPropertiesObj["spark.app.id"],
        "sun.java.command": systemPropertiesObj["sun.java.command"],
        "spark.master": sparkPropertiesObj["spark.master"],
        "javaVersion": runtimeObj["javaVersion"],
        "scalaVersion": runtimeObj["scalaVersion"]
    };
    return [appName, config]
}

function calcNodeType(name: string): NodeType {
    if(name === "Scan csv" || name === "Scan text") {
        return "input";
    } else if(name === "Execute InsertIntoHadoopFsRelationCommand") {
        return "output";
    } else if(name === "BroadcastHashJoin" || name === "SortMergeJoin" || name === "CollectLimit" || name === "BroadcastNestedLoopJoin") {
        return "join";
    } else if(name === "filter") {
        return "transformation";
    } else {
        return "other"
    }
}

const metricAllowlist: Record<NodeType, Array<string>> = {
    "input": ["number of output rows", "number of files", "size of files"],
    "output": ["number of written files", "number of output rows", "written output"],
    "join": ["number of output rows"],
    "transformation": ["number of output rows"],
    "other": []
}

function calcNodeMetrics(type: NodeType, metrics: EnrichedSqlMetric[]): EnrichedSqlMetric[] {
    const allowList = metricAllowlist[type];
    return metrics.filter(metric => allowList.includes(metric.name))
}

export function cleanUpDAG(edges: EnrichedSqlEdge[], nodes: EnrichedSqlNode[]): [EnrichedSqlEdge[], EnrichedSqlNode[]] {
    var g = new Graph();
    nodes.forEach(node => g.setNode(node.nodeId.toString()));
    edges.forEach(edge => g.setEdge(edge.fromId.toString(), edge.toId.toString()));

    const notVisibleNodes = nodes.filter(node => !node.isVisible)

    notVisibleNodes.forEach(node => {
        const nodeId = node.nodeId.toString()
        const inEdges = g.inEdges(nodeId) as Edge[];
        if (inEdges === undefined) {
            return;
        }
        const targets = (g.outEdges(nodeId) as Edge[]);
        if (targets === undefined || targets.length === 0) {
            return;
        }
        const target = targets[0]
        inEdges.forEach(inEdge => g.setEdge(inEdge.v, target.w));
        g.removeNode(nodeId)
        
    });

    const filteredEdges: EnrichedSqlEdge[] = g.edges().map((edge: Edge) => { return {fromId: parseInt(edge.v), toId: parseInt(edge.w)} });

    return [filteredEdges, nodes.filter(node => node.isVisible)]
}

function updateSqlMetrics(existingSql: EnrichedSparkSQL, sql: SparkSQL): EnrichedSparkSQL {
    const nodesWithUpdatedMetrics = existingSql.nodes.map(node => {
        const newNode = sql.nodes.filter(curNode => curNode.nodeId === node.nodeId)[0];
        const newMetrics = calcNodeMetrics(node.type, newNode.metrics as EnrichedSqlMetric[]);

        const updatedMetrics = newMetrics.map(newMetric => {
            const existingMetrics = node.metrics.filter(curMetric => curMetric.name === newMetric.name);
            if(existingMetrics.length === 0) {
                return newMetric;
            }
            const existingMetric = existingMetrics[0];
            if (newMetric.value === existingMetric.value) {
                return existingMetric;
            }
            return {...existingMetric, value: newMetric.value};
        });

        return {...node, metrics: updatedMetrics};
    });

    return {...existingSql, nodes: nodesWithUpdatedMetrics};
}

function calculateSql(sql: SparkSQL): EnrichedSparkSQL {
    const enrichedSql = sql as EnrichedSparkSQL;
    const typeEnrichedNodes = enrichedSql.nodes.map(node => {
        const type = calcNodeType(node.nodeName);
        return {...node, type: type, isVisible: type !== "other"};
    });

    if(typeEnrichedNodes.filter(node => node.type === 'output').length === 0) {
        // if there is no output, update the last node which is not "AdaptiveSparkPlan" or WholeStageCodegen to be the output
        const filtered = typeEnrichedNodes.filter(node => node.nodeName !== "AdaptiveSparkPlan" && !node.nodeName.includes("WholeStageCodegen"))
        const lastNode = filtered[filtered.length - 1];
        lastNode.type = 'output';
        lastNode.isVisible = true;
    }
    
    const [filteredEdges, filteredNodes] = cleanUpDAG(enrichedSql.edges, typeEnrichedNodes)

    const metricEnrichedNodes = filteredNodes.map(node => {
        return {...node, metrics: calcNodeMetrics(node.type, node.metrics)};
    });

    return {...enrichedSql, nodes: metricEnrichedNodes, edges: filteredEdges};
}

function calculateSqls(sqls: SparkSQLs): EnrichedSparkSQL[] {
    return sqls.map(calculateSql);
}

function calculateSqlStore(currentStore: SparkSQLStore | undefined, sqls: SparkSQLs): SparkSQLStore {
    if(currentStore === undefined) {
        return { sqls: calculateSqls(sqls) };
    }

    const currentLastSqlId = Math.max(...currentStore.sqls.map(sql => parseInt(sql.id)))
    const newLastSqlId = Math.max(...sqls.map(sql => parseInt(sql.id)))

    if(currentLastSqlId !== newLastSqlId) {
        return { sqls: [...currentStore.sqls, ...calculateSqls(sqls.slice(currentStore.sqls.length - 1))] };
    } 

    // if we already build the initial sql we don't need to rebuild the DAG
    const lastNewSql = sqls[sqls.length - 1];
    const lastCurrentSql = currentStore.sqls[currentStore.sqls.length - 1]
    const updatedCurrentSql = updateSqlMetrics(lastCurrentSql, lastNewSql)

    console.log(updatedCurrentSql.nodes.map(node => node.metrics));

    return { sqls: [...currentStore.sqls.slice(0, currentStore.sqls.length - 2), updatedCurrentSql] };
}

function calculateStatus(existingStore: StatusStore | undefined, stages: SparkStages): StatusStore {
    const stagesDataClean = stages.filter((stage: Record<string, any>) => stage.status != "SKIPPED")
    const totalActiveTasks = stagesDataClean.map((stage: Record<string, any>) => stage.numActiveTasks).reduce((a: number, b: number) => a + b, 0);
    const totalPendingTasks = stagesDataClean.map((stage: Record<string, any>) => stage.numTasks - stage.numActiveTasks - stage.numFailedTasks - stage.numCompleteTasks).reduce((a: number, b: number) => a + b, 0);
    const totalInput = stagesDataClean.map((stage: Record<string, any>) => stage.inputBytes).reduce((a: number, b: number) => a + b, 0);
    const totalOutput = stagesDataClean.map((stage: Record<string, any>) => stage.outputBytes).reduce((a: number, b: number) => a + b, 0);
    const status = totalActiveTasks == 0 ? "idle" : "working";

    const state: StatusStore = {
        totalActiveTasks: totalActiveTasks,
        totalPendingTasks: totalPendingTasks,
        totalInput: humanFileSize(totalInput),
        totalOutput: humanFileSize(totalOutput),
        status: status
    }

    if(existingStore === undefined) {
        return state;
    } else if(isEqual(state, existingStore)) {
        return existingStore;
    } else {
        return state;
    }
}


export function sparkApiReducer(store: AppStore, action: ApiAction): AppStore {
    switch (action.type) {
        case 'setInitial':
            const [appName, config] = extractConfig(action.config)
            return { ...store, isInitialized: true, appName: appName, appId: action.appId, sparkVersion: action.sparkVersion, config: config, status: undefined, sql: undefined };
        case 'setSQL':
            const sqlStore = calculateSqlStore(store.sql, action.value);
            if(sqlStore === store.sql) {
                return store;
            } else {
                return { ...store, sql: sqlStore };
            }
        case 'setStatus':
            const status = calculateStatus(store.status, action.value);
            if(status === store.status) {
                return store;
            } else {
                return { ...store, status: status };
            }
        default:
            return store;
    }
}
