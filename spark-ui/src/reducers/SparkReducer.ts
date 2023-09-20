import { ApiAction } from "../interfaces/APIAction";
import { AppStore, EnrichedSparkSQL, EnrichedSqlMetric, NodeType, SparkSQLStore, StatusStore, EnrichedSqlEdge, EnrichedSqlNode } from '../interfaces/AppStore';
import { SparkConfiguration } from "../interfaces/SparkConfiguration";
import { SparkSQL, SparkSQLs } from "../interfaces/SparkSQLs";
import { SparkStages } from "../interfaces/SparkStages";
import { humanFileSize } from "../utils/FormatUtils";
import isEqual from 'lodash/isEqual';
import { Edge, Graph } from 'graphlib';
import { v4 as uuidv4 } from 'uuid';
import { NodesMetrics } from "../interfaces/SqlMetrics";

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

function calculateSql(sql: SparkSQL): EnrichedSparkSQL {
    const enrichedSql = sql as EnrichedSparkSQL;
    const originalNumOfNodes = enrichedSql.nodes.length;
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
    
    const [filteredEdges, filteredNodes] = cleanUpDAG(enrichedSql.edges, typeEnrichedNodes);

    const metricEnrichedNodes = filteredNodes.map(node => {
        return {...node, metrics: calcNodeMetrics(node.type, node.metrics)};
    });

    return {...enrichedSql, nodes: metricEnrichedNodes, edges: filteredEdges, uniqueId: uuidv4(), metricUpdateId: uuidv4(), originalNumOfNodes: originalNumOfNodes};
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
        const newSqls = sqls.filter(sql => parseInt(sql.id) > currentLastSqlId);
        return { sqls: [...currentStore.sqls, ...calculateSqls(newSqls)] };
    }

    // if we already build the initial sql we don't need to rebuild the DAG
    const lastNewSql = sqls[sqls.length - 1];
    const lastCurrentSql = currentStore.sqls[currentStore.sqls.length - 1]

    if(lastNewSql.nodes.length !== lastCurrentSql.originalNumOfNodes) {
        // AQE changed the plan, we need to recalculate SQL
        const updatedSql = calculateSql(lastNewSql)
        return { sqls: [...currentStore.sqls.slice(0, currentStore.sqls.length - 2), updatedSql] };
    }

    return currentStore;
}

function updateSqlMetrics(currentStore: SparkSQLStore, sqlId: string, sqlMetrics: NodesMetrics): SparkSQLStore {
    const runningSqls = currentStore.sqls.filter(sql => sql.id === sqlId)
    if(runningSqls.length === 0) {
        // Shouldn't happen as if we ask for updated SQL metric we should have the SQL in store
        return currentStore;
    }

    const notEffectedSqls = currentStore.sqls.filter(sql => sql.id !== sqlId);
    const runningSql = runningSqls[0];
    const nodes = runningSql.nodes.map(node => {
        const matchedMetricsNodes = sqlMetrics.filter(nodeMetrics => nodeMetrics.id === node.nodeId);
        if(matchedMetricsNodes.length === 0) {
            return node;
        }
        // TODO: maybe do a smarter replacement, or send only the initialized metrics
        return {...node, metrics: calcNodeMetrics(node.type, matchedMetricsNodes[0].metrics)}
    })

    const updatedSql = {...runningSql, nodes: nodes, metricUpdateId: uuidv4()};
    return {...currentStore, sqls: [...notEffectedSqls, updatedSql ]};

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
        case 'setSQMetrics':
            if(store.sql === undefined) {
                // Shouldn't happen as store should be initialized when we get updated metrics
                return store;
            } else {
                return {...store, sql: updateSqlMetrics(store.sql, action.sqlId, action.value) };
            }
        default:
            return store;
    }
}
