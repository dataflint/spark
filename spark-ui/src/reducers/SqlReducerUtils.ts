import {
  EnrichedSqlMetric,
  NodeType,
  ParsedNodePlan,
} from "../interfaces/AppStore";

const metricAllowlist: Record<NodeType, Array<string>> = {
  input: [
    "number of output rows",
    "number of files read",
    "size of files read",
    "number of partitions read",
    "estimated number of fetched offsets out of range",
    "number of data loss error",
  ],
  output: [
    "number of written files",
    "number of output rows",
    "written output",
    "number of dynamic part",
  ],
  join: ["number of output rows"],
  transformation: ["number of output rows"],
  shuffle: [
    "number of partitions",
    "shuffle bytes written",
    "shuffle records written",
  ],
  broadcast: ["number of output rows", "data size"],
  sort: ["spill size"],
  other: [],
};

const metricsValueTransformer: Record<
  string,
  (value: string) => string | undefined
> = {
  "size of files read": extractTotalFromStatisticsMetric,
  "shuffle bytes written": extractTotalFromStatisticsMetric,
  "spill size": extractTotalFromStatisticsMetric,
  "number of dynamic part": (value: string) => {
    // if dynamic part is 0 we want to remove it from metrics
    if (value === "0") {
      return undefined;
    } else {
      return value;
    }
  },
};

const metricsRenamer: Record<string, string> = {
  "number of output rows": "rows",
  "number of written files": "files written",
  "written output": "bytes written",
  "number of files read": "files read",
  "size of files read": "bytes read",
  "number of partitions read": "partitions read",
  "number of dynamic part": "partitions written",
  "number of partitions": "partitions",
  "shuffle bytes written": "shuffle write",
  "estimated number of fetched offsets out of range":
    "fetched offsets out of range",
  "number of data loss error": "data loss error",
};

const nodeTypeDict: Record<string, NodeType> = {
  LocalTableScan: "input",
  Range: "input",
  "Execute InsertIntoHadoopFsRelationCommand": "output",
  CollectLimit: "output",
  TakeOrderedAndProject: "output",
  BroadcastHashJoin: "join",
  SortMergeJoin: "join",
  BroadcastNestedLoopJoin: "join",
  ShuffleHashJoin: "join",
  Filter: "transformation",
  Union: "join",
  Exchange: "shuffle",
  AQEShuffleRead: "shuffle",
  HashAggregate: "transformation",
  BroadcastExchange: "broadcast",
  Sort: "sort",
  Project: "transformation",
  Window: "transformation",
};

const nodeRenamerDict: Record<string, string> = {
  HashAggregate: "Aggregate",
  "Execute InsertIntoHadoopFsRelationCommand": "Write to HDFS",
  LocalTableScan: "Read in-memory table",
  "Execute RepairTableCommand": "Repair table",
  "Execute CreateDataSourceTableCommand": "Create table",
  "Execute CreateDataSourceTableAsSelectCommand": "Create table with Select",
  "Execute DropTableCommand": "Drop table",
  SetCatalogAndNamespace: "Set database",
  TakeOrderedAndProject: "Take Ordered",
  CollectLimit: "Collect",
  BroadcastHashJoin: "Join (Broadcast Hash)",
  SortMergeJoin: "Join (Sort Merge)",
  BroadcastNestedLoopJoin: "Join (Broadcast Nested Loop)",
  CreateNamespace: "Create Catalog Namespace",
  Exchange: "Repartition",
  AQEShuffleRead: "Runtime Partition Coalescing",
  BroadcastExchange: "Broadcast",
  Project: "Select",
  MicroBatchScan: "Read Micro batch",
  ShuffleHashJoin: "Join (Shuffle Hash)",
};

export function extractTotalFromStatisticsMetric(
  value: string | undefined,
): string | undefined {
  if (value === undefined) {
    return undefined;
  }
  const newlineSplit = value.split("\n");
  if (newlineSplit.length < 2) {
    return value;
  }
  const bracetSplit = newlineSplit[1].split("(");
  if (bracetSplit.length === 0) {
    return value;
  }

  return bracetSplit[0].trim();
}

export function nodeEnrichedNameBuilder(
  name: string,
  plan: ParsedNodePlan | undefined,
): string {
  if (plan !== undefined) {
    switch (plan.type) {
      case "HashAggregate":
        return (
          "Aggregate" +
          (plan.plan.operations.length > 0 && plan.plan.operations.length < 3
            ? ` (${plan.plan.operations.join(", ")})`
            : "")
        );
      case "Exchange":
        if (plan.plan.type === "hashpartitioning") {
          return `Repartition By Hash`;
        } else if (plan.plan.type === "rangepartitioning") {
          return `Repartition By Range`;
        } else if (plan.plan.type === "SinglePartition") {
          return "Repartition To Single Partition";
        } else if (plan.plan.type === "RoundRobinPartitioning") {
          return "Repartition By Round Robin";
        }
    }
  }

  const renamedNodeName = nodeRenamerDict[name];
  if (renamedNodeName !== undefined) {
    return renamedNodeName;
  }
  if (name.includes("Scan")) {
    const scanRenamed = name.replace("Scan", "Read");
    const scanNameSliced = scanRenamed.split(" ");
    if (scanNameSliced.length > 2) {
      return scanNameSliced.slice(0, 2).join(" ");
    }
    return scanRenamed;
  }
  return name;
}

export function calcNodeMetrics(
  type: NodeType,
  metrics: EnrichedSqlMetric[],
): EnrichedSqlMetric[] {
  const allowList = metricAllowlist[type];
  return (
    metrics
      .filter((metric) => allowList.includes(metric.name))
      .map((metric) => {
        const valueTransformer = metricsValueTransformer[metric.name];
        if (valueTransformer === undefined) {
          return metric;
        }
        const valueTransformed = valueTransformer(metric.value);
        return valueTransformed === undefined
          ? undefined
          : { ...metric, value: valueTransformed };
      })
      .filter((metric) => metric !== undefined) as EnrichedSqlMetric[]
  ).map((metric) => {
    const metricNameRenamed = metricsRenamer[metric.name];
    return metricNameRenamed === undefined
      ? metric
      : { ...metric, name: metricNameRenamed };
  });
}

export function calcNodeType(name: string): NodeType {
  if (name.includes("Scan")) {
    return "input";
  }
  const renamedName = nodeTypeDict[name];
  if (renamedName === undefined) {
    return "other";
  }
  return renamedName;
}
