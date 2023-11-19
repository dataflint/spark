import { Edge, Graph } from "graphlib";
import { v4 as uuidv4 } from "uuid";
import {
  EnrichedSparkSQL,
  EnrichedSqlEdge,
  EnrichedSqlNode,
  ParsedNodePlan,
  SparkSQLStore,
} from "../interfaces/AppStore";
import { SQLNodePlan, SQLPlan, SQLPlans } from "../interfaces/SQLPlan";
import { SparkSQL, SparkSQLs, SqlStatus } from "../interfaces/SparkSQLs";
import { NodesMetrics } from "../interfaces/SqlMetrics";
import { timeStrToEpocTime } from "../utils/FormatUtils";
import { parseCollectLimit } from "./PlanParsers/CollectLimitParser";
import { parseExchange } from "./PlanParsers/ExchangeParser";
import { parseFilter } from "./PlanParsers/FilterParser";
import { parseJoin } from "./PlanParsers/JoinParser";
import { parseProject } from "./PlanParsers/ProjectParser";
import { parseFileScan } from "./PlanParsers/ScanFileParser";
import { parseSort } from "./PlanParsers/SortParser";
import { parseTakeOrderedAndProject } from "./PlanParsers/TakeOrderedAndProjectParser";
import { parseWriteToHDFS } from "./PlanParsers/WriteToHDFSParser";
import { parseHashAggregate } from "./PlanParsers/hashAggregateParser";
import {
  calcNodeMetrics,
  calcNodeType,
  nodeEnrichedNameBuilder,
} from "./SqlReducerUtils";

export function cleanUpDAG(
  edges: EnrichedSqlEdge[],
  allNodes: EnrichedSqlNode[],
  visibleNodes: EnrichedSqlNode[]
): EnrichedSqlEdge[] {
  var g = new Graph();
  allNodes.forEach((node) => g.setNode(node.nodeId.toString()));
  edges.forEach((edge) =>
    g.setEdge(edge.fromId.toString(), edge.toId.toString()),
  );

  const visibleNodesIds = visibleNodes.map((node) => node.nodeId);
  const notVisibleNodes = allNodes.filter((node) => !visibleNodesIds.includes(node.nodeId));

  notVisibleNodes.forEach((node) => {
    const nodeId = node.nodeId.toString();
    const inEdges = g.inEdges(nodeId) as Edge[];
    if (inEdges === undefined) {
      return;
    }
    const targets = g.outEdges(nodeId) as Edge[];
    if (targets === undefined || targets.length === 0) {
      return;
    }
    const target = targets[0];
    inEdges.forEach((inEdge) => g.setEdge(inEdge.v, target.w));
    g.removeNode(nodeId);
  });

  const filteredEdges: EnrichedSqlEdge[] = g.edges().map((edge: Edge) => {
    return { fromId: parseInt(edge.v), toId: parseInt(edge.w) };
  });

  const removeRedundentEdges = filteredEdges.filter(edge => visibleNodesIds.includes(edge.toId) && visibleNodesIds.includes(edge.fromId))

  return removeRedundentEdges;
}

export function parseNodePlan(
  node: EnrichedSqlNode,
  plan: SQLNodePlan,
): ParsedNodePlan | undefined {
  try {
    switch (node.nodeName) {
      case "HashAggregate":
        return {
          type: "HashAggregate",
          plan: parseHashAggregate(plan.planDescription),
        };
      case "TakeOrderedAndProject":
        return {
          type: "TakeOrderedAndProject",
          plan: parseTakeOrderedAndProject(plan.planDescription),
        };
      case "CollectLimit":
        return {
          type: "CollectLimit",
          plan: parseCollectLimit(plan.planDescription),
        };
      case "Execute InsertIntoHadoopFsRelationCommand":
        return {
          type: "WriteToHDFS",
          plan: parseWriteToHDFS(plan.planDescription),
        };
      case "Filter":
        return {
          type: "Filter",
          plan: parseFilter(plan.planDescription)
        }
      case "Exchange":
        return {
          type: "Exchange",
          plan: parseExchange(plan.planDescription)
        }
      case "Project":
        return {
          type: "Project",
          plan: parseProject(plan.planDescription)
        }
      case "Sort":
        return {
          type: "Sort",
          plan: parseSort(plan.planDescription)
        }
    }
    if (node.nodeName.includes("Scan")) {
      return {
        type: "FileScan",
        plan: parseFileScan(plan.planDescription, node.nodeName),
      };
    }
    if (node.nodeName.includes("Join")) {
      return {
        type: "Join",
        plan: parseJoin(plan.planDescription),
      };
    }
  } catch (e) {
    console.log(`failed to parse plan for node type: ${node.nodeName}`, e);
  }
  return undefined;
}

function calculateSql(
  sql: SparkSQL,
  plan: SQLPlan | undefined,
): EnrichedSparkSQL {
  const enrichedSql = sql as EnrichedSparkSQL;
  const originalNumOfNodes = enrichedSql.nodes.length;
  const typeEnrichedNodes = enrichedSql.nodes.map((node) => {
    const type = calcNodeType(node.nodeName);
    const nodePlan = plan?.nodesPlan.find(
      (planNode) => planNode.id === node.nodeId,
    );
    const parsedPlan =
      nodePlan !== undefined ? parseNodePlan(node, nodePlan) : undefined;
    const isCodegenNode = node.nodeName.includes("WholeStageCodegen");
    return {
      ...node,
      type: type,
      parsedPlan: parsedPlan,
      enrichedName: nodeEnrichedNameBuilder(node.nodeName, parsedPlan),
      isCodegenNode: isCodegenNode,
      wholeStageCodegenId: isCodegenNode ? extractCodegenId() : node.wholeStageCodegenId,
    };

    function extractCodegenId(): number | undefined {
      return parseInt(node.nodeName.replace("WholeStageCodegen (", "").replace(")", ""));
    }
  });

  const onlyGraphNodes = typeEnrichedNodes.filter(
    (node) =>
      !node.isCodegenNode,
  );

  if (onlyGraphNodes.filter((node) => node.type === "output").length === 0) {
    const aqeFilteredNodes = onlyGraphNodes.filter(node => node.nodeName !== "AdaptiveSparkPlan");
    const lastNode = aqeFilteredNodes[aqeFilteredNodes.length - 1];
    lastNode.type = "output";
  }

  const metricEnrichedNodes = onlyGraphNodes.map((node) => {
    return { ...node, metrics: calcNodeMetrics(node.type, node.metrics) };
  });

  const ioNodes = onlyGraphNodes.filter(node => node.type === "input" || node.type === "output" || node.type === "join");
  const basicNodes = onlyGraphNodes.filter(node => node.type === "input" || node.type === "output" || node.type === "join" || node.type === "transformation");
  const advancedNodes = onlyGraphNodes.filter(node => node.type !== "other");
  const ioNodesIds = ioNodes.map(node => node.nodeId);
  const basicNodesIds = basicNodes.map(node => node.nodeId);
  const advancedNodesIds = advancedNodes.map(node => node.nodeId);

  const basicFilteredEdges = cleanUpDAG(
    enrichedSql.edges,
    onlyGraphNodes,
    basicNodes
  );

  const advancedFilteredEdges = cleanUpDAG(
    enrichedSql.edges,
    onlyGraphNodes,
    advancedNodes
  );

  const ioFilteredEdges = cleanUpDAG(
    enrichedSql.edges,
    onlyGraphNodes,
    ioNodes
  );

  const isSqlCommand =
    sql.runningJobIds.length === 0 &&
    sql.failedJobIds.length === 0 &&
    sql.successJobIds.length === 0;

  return {
    ...enrichedSql,
    nodes: metricEnrichedNodes,
    filters: {
      "io": {
        nodesIds: ioNodesIds,
        edges: ioFilteredEdges
      },
      "basic": {
        nodesIds: basicNodesIds,
        edges: basicFilteredEdges
      },
      "advanced": {
        nodesIds: advancedNodesIds,
        edges: advancedFilteredEdges
      },
    },
    uniqueId: uuidv4(),
    metricUpdateId: uuidv4(),
    isSqlCommand: isSqlCommand,
    originalNumOfNodes: originalNumOfNodes,
    submissionTimeEpoc: timeStrToEpocTime(sql.submissionTime),
  };
}

function calculateSqls(sqls: SparkSQLs, plans: SQLPlans): EnrichedSparkSQL[] {
  return sqls.map((sql) => {
    const plan = plans.find((plan) => plan.executionId === parseInt(sql.id));
    return calculateSql(sql, plan);
  });
}

export function calculateSqlStore(
  currentStore: SparkSQLStore | undefined,
  sqls: SparkSQLs,
  plans: SQLPlans,
): SparkSQLStore {
  if (currentStore === undefined) {
    return { sqls: calculateSqls(sqls, plans) };
  }

  const sqlIds = sqls.map((sql) => parseInt(sql.id));
  const minId = Math.min(...sqlIds);

  // add existing completed IDs
  let updatedSqls: EnrichedSparkSQL[] = currentStore.sqls.filter(sql => parseInt(sql.id) < minId)

  for (const id of sqlIds) {
    const newSql = sqls.find(
      (existingSql) => parseInt(existingSql.id) === id,
    ) as SparkSQL;
    const currentSql = currentStore.sqls.find(
      (existingSql) => parseInt(existingSql.id) === id,
    );
    const plan = plans.find((plan) => plan.executionId === id);

    // case 1: SQL does not exist, we add it
    if (currentSql === undefined) {
      updatedSqls.push(calculateSql(newSql, plan));
      // From here currentSql must not be null, and currentSql can't be COMPLETED as it would not be requested by API
      // case 2: plan status changed from running to completed, so we need to update the SQL
    } else if (
      newSql.status === SqlStatus.Completed.valueOf() ||
      newSql.status === SqlStatus.Failed.valueOf()
    ) {
      updatedSqls.push(calculateSql(newSql, plan));
      // From here newSql.status must be RUNNING
      // case 3: running SQL structure, so we need to update the plan
    } else if (currentSql.originalNumOfNodes !== newSql.nodes.length) {
      updatedSqls.push(calculateSql(newSql, plan));
    } else {
      // case 4: SQL is running, but the structure haven't changed, so we update only relevant fields
      updatedSqls.push({
        ...currentSql,
        duration: newSql.duration,
        failedJobIds: newSql.failedJobIds,
        runningJobIds: newSql.runningJobIds,
        successJobIds: newSql.successJobIds,
      });
    }
  }

  return { sqls: updatedSqls };
}

export function updateSqlNodeMetrics(
  currentStore: SparkSQLStore,
  sqlId: string,
  sqlMetrics: NodesMetrics,
): SparkSQLStore {
  const runningSqls = currentStore.sqls.filter((sql) => sql.id === sqlId);
  if (runningSqls.length === 0) {
    // Shouldn't happen as if we ask for updated SQL metric we should have the SQL in store
    return currentStore;
  }

  const notEffectedSqls = currentStore.sqls.filter((sql) => sql.id !== sqlId);
  const runningSql = runningSqls[0];
  const nodes = runningSql.nodes.map((node) => {
    const matchedMetricsNodes = sqlMetrics.filter(
      (nodeMetrics) => nodeMetrics.id === node.nodeId,
    );
    if (matchedMetricsNodes.length === 0) {
      return node;
    }
    // TODO: maybe do a smarter replacement, or send only the initialized metrics
    return {
      ...node,
      metrics: calcNodeMetrics(node.type, matchedMetricsNodes[0].metrics),
    };
  });

  const updatedSql = { ...runningSql, nodes: nodes, metricUpdateId: uuidv4() };
  return { ...currentStore, sqls: [...notEffectedSqls, updatedSql] };
}
