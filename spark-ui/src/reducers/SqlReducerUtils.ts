import {
  EnrichedSqlMetric,
  NodeType,
  ParsedNodePlan,
} from "../interfaces/AppStore";
import { humanFileSize } from "../utils/FormatUtils";

const metricAllowlist: Record<NodeType, Array<string>> = {
  input: [
    "number of output rows",
    "total data file size (bytes)",
    "number of files read",
    "size of files read",
    "number of partitions read",
    "estimated number of fetched offsets out of range",
    "number of data loss error",
    "total data manifests",
    "number of file splits read",
    "output columnar batches",
    "number of bytes pruned",
    "number of files pruned",
  ],
  output: [
    "number of written files",
    "number of output rows",
    "written output",
    "number of dynamic part",
  ],
  join: ["number of output rows", "output columnar batches"],
  transformation: [
    "number of output rows",
    "output columnar batches",
    "output rows",
    "data sent to Python workers",
    "data returned from Python workers",
  ],
  shuffle: [
    "number of partitions",
    "shuffle bytes written",
    "shuffle records written",
    "number of output rows",
    "num bytes read",
    "num bytes written",
    "output columnar batches",
    "partition data size",
  ],

  broadcast: ["number of output rows", "data size", "output columnar batches"],
  sort: ["spill size", "output columnar batches"],
  other: [],
};

const metricsValueTransformer: Record<
  string,
  (value: string) => string | undefined
> = {
  "size of files read": extractTotalFromStatisticsMetric,
  "shuffle bytes written": extractTotalFromStatisticsMetric,
  "num bytes read": extractTotalFromStatisticsMetric,
  "spill size": extractTotalFromStatisticsMetric,
  "total data file size (bytes)": bytesToHumanReadableSize,
  "partition data size": extractTotalFromStatisticsMetric,
  "data sent to Python workers": extractTotalFromStatisticsMetric,
  "data returned from Python workers": extractTotalFromStatisticsMetric,
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
  "output rows": "rows",
  "number of written files": "files written",
  "written output": "bytes written",
  "number of files read": "files read",
  "size of files read": "bytes read",
  "num bytes read": "bytes read",
  "num bytes written": "bytes written",
  "number of partitions read": "partitions read",
  "number of dynamic part": "partitions written",
  "number of partitions": "partitions",
  "shuffle bytes written": "shuffle write",
  "estimated number of fetched offsets out of range":
    "fetched offsets out of range",
  "number of data loss error": "data loss error",
  "total data file size (bytes)": "bytes read",
  "total data manifests": "data manifests read",
  "number of file splits read": "files read",
  "output columnar batches": "output batches",
  "partition data size": "data size",
  "data sent to Python workers": "data sent",
  "data returned from Python workers": "data returned",
  "number of bytes pruned": "bytes pruned",
  "number of files pruned": "files pruned",
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
  "SortMergeJoin(skew=true)": "join",
  Exchange: "shuffle",
  AQEShuffleRead: "shuffle",
  HashAggregate: "transformation",
  BroadcastExchange: "broadcast",
  Sort: "sort",
  Project: "transformation",
  Window: "transformation",
  AppendData: "output",
  ReplaceData: "output",
  WriteDelta: "output",
  DeleteFromTable: "output",
  PhotonProject: "transformation",
  PhotonGroupingAgg: "transformation",
  PhotonShuffleExchangeSink: "shuffle",
  PhotonShuffleExchangeSource: "shuffle",
  PhotonTopK: "output",
  PhotonFilter: "transformation",
  GpuFilter: "transformation",
  GpuBroadcastHashJoin: "join",
  GpuCoalesceBatches: "shuffle",
  GpuBroadcastExchange: "broadcast",
  GpuProject: "transformation",
  GpuHashAggregate: "transformation",
  GpuColumnarExchange: "shuffle",
  GpuCustomShuffleReader: "shuffle",
  GpuTopN: "output",
  GpuShuffleCoalesce: "shuffle",
  GpuSort: "sort",
  GpuShuffledSymmetricHashJoin: "join",
  GpuBroadcastNestedLoopJoin: "join",
  CometColumnarExchange: "shuffle",
  CometHashAggregate: "transformation",
  CometExchange: "shuffle",
  CometProject: "transformation",
  CometFilter: "transformation",
  CometSort: "sort",
  CometHashJoin: "join",
  CometBroadcastHashJoin: "join",
  CometSortMergeJoin: "join",
  Coalesce: "shuffle",
  PhotonBroadcastExchange: "broadcast",
  PhotonBroadcastHashJoin: "join",
  CartesianProduct: "join",
  InMemoryTableScan: "transformation",
  MapInPandas: "transformation",
  ArrowEvalPython: "transformation",
  FlatMapGroupsInPandas: "transformation",
  BatchEvalPython: "transformation",
  Generate: "transformation",
  Expand: "transformation",
};

const nodeRenamerDict: Record<string, string> = {
  HashAggregate: "Aggregate",
  "Execute InsertIntoHadoopFsRelationCommand": "Write to HDFS",
  LocalTableScan: "Read in-memory table",
  "Execute RepairTableCommand": "Repair table",
  "Execute CreateDataSourceTableCommand": "Create table",
  "Execute CreateDataSourceTableAsSelectCommand": "Create table with Select",
  "Execute DropTableCommand": "Drop table",
  "Execute AddJarsCommand": "Add jars",
  "SortMergeJoin(skew=true)": "Join (Sort Merge) (Skew)",
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
  DropTable: "Drop table",
  CreateTable: "Create table",
  AppendData: "Iceberg - Append data",
  ReplaceData: "Iceberg - Replace data",
  WriteDelta: "Iceberg - Write Delta",
  DeleteFromTable: "Iceberg - Delete from table",
  PhotonProject: "Project (Photon)",
  PhotonGroupingAgg: "Aggregate (Photon)",
  PhotonShuffleExchangeSink: "Exchange Write (Photon)",
  PhotonShuffleExchangeSource: "Exchange Read (Photon)",
  PhotonTopK: "Take (Photon)",
  PhotonFilter: "Filter (Photon)",
  GpuFilter: "Filter (RAPIDS)",
  GpuCoalesceBatches: "Coalesce Batches (RAPIDS)",
  GpuBroadcastExchange: "Broadcast (RAPIDS)",
  GpuProject: "Project (RAPIDS)",
  GpuBroadcastHashJoin: "Join (Broadcast Hash) (RAPIDS)",
  GpuHashAggregate: "Aggregate (RAPIDS)",
  GpuColumnarExchange: "Exchange (RAPIDS)",
  GpuCustomShuffleReader: "Shuffle Read (RAPIDS)",
  GpuTopN: "Take (RAPIDS)",
  GpuShuffleCoalesce: "Coalesce (RAPIDS)",
  GpuSort: "Sort (RAPIDS)",
  GpuShuffledSymmetricHashJoin: "Join (Shuffled Symmetric Hash) (RAPIDS)",
  GpuBroadcastNestedLoopJoin: "Join (Broadcast Nested Loop) (RAPIDS)",
  CometProject: "Project (Comet)",
  CometHashAggregate: "Aggregate (Comet)",
  CometExchange: "Exchange (Comet)",
  CometColumnarExchange: "Columnar Exchange (Comet)",
  CometFilter: "Filter (Comet)",
  CometSort: "Sort (Comet)",
  CometHashJoin: "Join (Comet)",
  CometBroadcastHashJoin: "Join (Broadcast Hash) (Comet)",
  CometSortMergeJoin: "Join (Sort Merge) (Comet)",
  Coalesce: "Coalesce",
  PhotonBroadcastExchange: "Broadcast (Photon)",
  PhotonBroadcastHashJoin: "Join (Broadcast Hash) (Photon)",
  CartesianProduct: "Join (Cartesian Product)",
  InMemoryTableScan: "Cache",
  MapInPandas: "Select (with Pandas)",
  ArrowEvalPython: "Select (with Arrow)",
  FlatMapGroupsInPandas: "Select Flat (with Pandas)",
  BatchEvalPython: "Run Python UDF",
  Expand: "Expand",
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

export function bytesToHumanReadableSize(
  value: string | undefined,
): string | undefined {
  if (value === undefined) {
    return undefined;
  }
  try {
    return humanFileSize(parseInt(value, 10));
  } catch (e) {
    console.log(`failed to parse ${value} to a number`); // shouldn't happen
    return value;
  }
}

function capitalizeFirst(str: string): string {
  if (str.length === 0) return str;
  return str.charAt(0).toUpperCase() + str.slice(1);
}

function getCommonOperationPrefix(operations: string[]): string | null {
  if (operations.length === 0) {
    return null;
  }

  if (operations.length === 1) {
    return operations[0];
  }

  // Check if all operations are the same
  const firstOperation = operations[0];
  const allSame = operations.every((op) => op === firstOperation);

  return allSame ? firstOperation : null;
}

export function nodeEnrichedNameBuilder(
  name: string,
  plan: ParsedNodePlan | undefined,
): string {
  if (plan !== undefined) {
    switch (plan.type) {
      case "HashAggregate":
        if (plan.plan.functions.length == 0) {
          return "Distinct";
        }

        // Build enriched name based on operations
        if (plan.plan.operations.length > 0) {
          // Extract common prefix from all operations
          const commonPrefix = getCommonOperationPrefix(plan.plan.operations);

          if (commonPrefix) {
            // Use common prefix as the operation name
            let operationName: string;
            if (commonPrefix.startsWith("partial_")) {
              const baseName = commonPrefix.substring(8);
              operationName = capitalizeFirst(baseName) + " within partition"; // Remove "partial_" and add suffix
            } else if (commonPrefix.startsWith("merge_")) {
              const baseName = commonPrefix.substring(6);
              operationName = capitalizeFirst(baseName) + " by merge"; // Remove "merge_" and add suffix
            } else if (commonPrefix.startsWith("finalmerge_")) {
              const baseName = commonPrefix.substring(11);
              operationName = capitalizeFirst(baseName) + " by merge"; // Remove "finalmerge_" and add suffix
            } else {
              operationName = capitalizeFirst(commonPrefix);
            }
            return `${operationName}`;
          } else if (plan.plan.operations.length < 3) {
            // Fallback to showing individual operations if no common prefix and few operations
            const formattedOperations = plan.plan.operations.map((op) => {
              if (op.startsWith("partial_")) {
                const baseName = op.substring(8);
                return capitalizeFirst(baseName) + " within partition";
              } else if (op.startsWith("merge_")) {
                const baseName = op.substring(6);
                return capitalizeFirst(baseName) + " by merge";
              } else if (op.startsWith("finalmerge_")) {
                const baseName = op.substring(11);
                return capitalizeFirst(baseName) + " by merge";
              } else {
                return capitalizeFirst(op);
              }
            });
            return `Aggregate (${formattedOperations.join(", ")})`;
          }
        }

        return "Aggregate";
      case "Generate":
        if (plan?.plan?.operation !== undefined) {
          return plan.plan.operation;
        }
        return "Generate";
      case "Expand":
        return "Expand";
      case "Exchange":
        if (plan.plan.isBroadcast) {
          return "Broadcast";
        } else if (plan.plan.type === "hashpartitioning") {
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
    let scanRenamed = name.includes("BatchScan")
      ? name.replace("BatchScan", "Read")
      : name.replace("Scan", "Read");
    scanRenamed = scanRenamed.replace("GpuRead", "Read (RAPIDS)");
    const scanNameSliced = scanRenamed.split(" ");
    if (scanNameSliced.length > 2) {
      return scanNameSliced.slice(0, 2).join(" ");
    }
    const scanNameTrancated =
      scanRenamed.length > 30 ? scanRenamed.slice(0, 30) + "..." : scanRenamed;
    return scanNameTrancated;
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
