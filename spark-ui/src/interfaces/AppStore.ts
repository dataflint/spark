import { EnvironmentInfo } from './ApplicationInfo';
import { RddStorageInfo } from './CachedStorage';
import { DeltaLakeScanInfo } from "./DeltaLakeInfo";
import { IcebergCommitsInfo } from "./IcebergInfo";

export type NodeType =
  | "input"
  | "output"
  | "transformation"
  | "join"
  | "shuffle"
  | "broadcast"
  | "sort"
  | "other";

export type AppStore = {
  isConnected: boolean;
  isInitialized: boolean;
  connectionErrorMessage: string | undefined;
  runMetadata: RunMetadataStore | undefined;
  config: ConfigStore | undefined;
  status: StatusStore | undefined;
  sql: SparkSQLStore | undefined;
  jobs: SparkJobsStore | undefined;
  stages: SparkStagesStore | undefined;
  executors: SparkExecutorsStore | undefined;
  executorTimeline: ExecutorTimelinePoints | undefined;
  alerts: AlertsStore | undefined;
  environmentInfo: EnvironmentInfo | undefined;
};

export interface AlertsStore {
  alerts: Alerts;
}

export type Alerts = Alert[];

export interface SQLAlertSourceData {
  type: "sql";
  sqlId: string;
  sqlNodeId: number;
}

export interface StageAlertSourceData {
  type: "stage";
  sqlId: string;
  stageId: number;
}

export interface StatusAlertSourceData {
  type: "status";
  metric: "memory" | "driverMemory" | "idleCores";
}

export interface Alert {
  id: string;
  name: string;
  title: string;
  message: string;
  location: string;
  shortSuggestion?: string | undefined;
  suggestion: string;
  type: AlertType;
  source: StatusAlertSourceData | SQLAlertSourceData | StageAlertSourceData;
}

export type AlertType = "error" | "warning";

export type ConfigCategory =
  | "general"
  | "resources"
  | "executor-memory"
  | "static-allocation"
  | "dynamic-allocation"
  | "dynamic-allocation-advanced"
  | "dynamic-allocation-super-advanced"
  | "databricks-autoscale"
  | "databricks-static-allocation";

export type ResourceMode =
  | "local"
  | "static"
  | "dynamic"
  | "databricks"
  | "unknown";

export interface ConfigEntry {
  name: string;
  key: string | undefined;
  value: string | undefined;
  default: string | undefined;
  category: ConfigCategory;
  documentation: string;
}

export type ConfigEntries = ConfigEntry[];

export interface ConfigStore {
  alertDisabled: string | undefined;
  resourceControlType: ResourceMode;
  configs: ConfigEntries;
  executorMemoryBytes: number;
  executorMemoryBytesString: string;
  executorMemoryBytesSparkFormatString: string;
  executorMemoryOverheadBytes: number;
  executorContainerMemoryBytes: number;
  executorMemoryOverheadFactor: number;
  executorMemoryOverheadViaConfigString: number;
  executorMemoryOverheadString: string;
  executorContainerMemoryString: string;
  executorMemoryOverheadViaConfig: number;
  driverMemoryBytes: number;
  driverMemoryBytesString: string;
  driverMemoryBytesSparkFormatString: string;
}

export interface RunMetadataStore {
  appId: string;
  sparkVersion: string;
  appName: string;
  startTime: number;
  endTime: number | undefined;
}

export interface StatusStore {
  stages: StagesSummeryStore | undefined;
  executors: SparkExecutorsStatus | undefined;
  sqlIdleTime: number;
  duration: number;
}

export interface StagesSummeryStore {
  totalActiveTasks: number;
  totalPendingTasks: number;
  totalInput: string;
  totalOutput: string;
  totalShuffleRead: string;
  totalShuffleWrite: string;
  totalDiskSpill: string;
  status: string;
  totalTaskTimeMs: number;
  taskErrorRate: number;
  totalTasks: number;
  totalFailedTasks: number;
}

export interface SparkSQLStore {
  sqls: EnrichedSparkSQL[];
}

export type GraphFilter = "io" | "basic" | "advanced";

export interface FilteredGraph {
  nodesIds: number[];
  edges: EnrichedSqlEdge[];
}

export interface EnrichedSparkSQL {
  id: string;
  uniqueId: string;
  metricUpdateId: string;
  // sql command is if the sql is not a query but a technical command like creating view and such
  isSqlCommand: boolean;
  originalNumOfNodes: number;
  // undefined for seperating the reducers more cleanly
  stageMetrics: SparkMetricsStore | undefined;
  resourceMetrics: SparkSQLResourceUsageStore | undefined;
  status: string;
  description: string;
  planDescription: string;
  submissionTime: string;
  submissionTimeEpoc: number;
  duration: number;
  runningJobIds: number[];
  successJobIds: number[];
  failedJobIds: number[];
  nodes: EnrichedSqlNode[];
  codegenNodes: EnrichedSqlNode[];
  edges: EnrichedSqlEdge[];
  filters: Record<GraphFilter, FilteredGraph>;
  failureReason: string | undefined;
  rootExecutionId: number | undefined;
}

export type ParsedHashAggregatePlan = {
  keys: string[];
  functions: string[];
  operations: string[];
};

export type ParsedTakeOrderedAndProjectPlan = {
  output: string[];
  orderBy: string[];
  limit: number;
};

export type ParseFileScanPlan = {
  Location?: string;
  tableName?: string;
  orderBy?: string[];
  PartitionFilters?: string[];
  PushedFilters?: string[];
  format?: string;
  ReadSchema?: { [key: string]: string };
};

export type ParsedJDBCScanPlan = {
  numPartitions?: number;
  tableName?: string;
  isFullScan?: boolean;
  isCustomQuery?: boolean;
  customQuery?: string;
  sql?: string;
  pushedFilters?: string[];
  selectedColumns?: string[];
  readSchema?: { [key: string]: string };
};

export type ParsedProjectPlan = {
  fields: string[];
};

export type ParseFilterPlan = {
  condition: string;
};

export type DeltaOptimizeWrite = {
  minFileSize: string;
  maxFileSize: string;
  autoCompactMinFileSize: string;
  autoCompactMaxFileSize: string;
};

export type ParsedExchangePlan = {
  type: string;
  fields: string[] | undefined;
  isBroadcast: boolean;
  deltaOptimizeWrite?: DeltaOptimizeWrite;
};

export type ParsedWindowPlan = {
  partitionFields: string[];
  selectFields: string[];
  sortFields: string[];
};

export interface ParsedJoinPlan {
  joinType: string;
  joinSideType: string;
  leftKeys?: string[];
  rightKeys?: string[];
  joinCondition?: string;
}

export type ParsedWriteToHDFSPlan = {
  location: string;
  format: string;
  mode: string;
  tableName?: string;
  partitionKeys?: string[];
};

export type ParsedWriteToDeltaPlan = {
  location: string;
  fields: string[];
};

export type ParsedCollectLimitPlan = {
  limit: number;
};

export type ParsedSortPlan = {
  fields: string[];
};

export interface ParsedCoalescePlan {
  partitionNum: number;
}

export type ParsedBatchEvalPythonPlan = {
  functionNames: string[];
  udfNames: string[];
};

export type ParsedGeneratePlan = {
  operation: string;
  explodeByField: string;
  explodeField: string[];
  selectedFields: string[];
};

export type ParsedExpandPlan = {
  fields: string[];
  idField: string;
};

export type ParsedNodePlan =
  | { type: "HashAggregate"; plan: ParsedHashAggregatePlan }
  | { type: "ObjectHashAggregate"; plan: ParsedHashAggregatePlan }
  | { type: "TakeOrderedAndProject"; plan: ParsedTakeOrderedAndProjectPlan }
  | { type: "CollectLimit"; plan: ParsedCollectLimitPlan }
  | { type: "FileScan"; plan: ParseFileScanPlan }
  | { type: "JDBCScan"; plan: ParsedJDBCScanPlan }
  | { type: "WriteToHDFS"; plan: ParsedWriteToHDFSPlan }
  | { type: "WriteToDelta"; plan: ParsedWriteToDeltaPlan }
  | { type: "Filter"; plan: ParseFilterPlan }
  | { type: "Project"; plan: ParsedProjectPlan }
  | { type: "Exchange"; plan: ParsedExchangePlan }
  | { type: "Join"; plan: ParsedJoinPlan }
  | { type: "Sort"; plan: ParsedSortPlan }
  | { type: "Window"; plan: ParsedWindowPlan }
  | { type: "Coalesce"; plan: ParsedCoalescePlan }
  | { type: "BatchEvalPython"; plan: ParsedBatchEvalPythonPlan }
  | { type: "Generate"; plan: ParsedGeneratePlan }
  | { type: "Expand"; plan: ParsedExpandPlan };


export interface ExchangeMetrics {
  writeDuration: number;
  readDuration: number;
  duration: number;
}
export interface EnrichedSqlNode {
  nodeId: number;
  nodeName: string;
  enrichedName: string;
  metrics: EnrichedSqlMetric[];
  type: NodeType;
  wholeStageCodegenId?: number;
  isCodegenNode: boolean;
  codegenDuration: number | undefined;
  exchangeBroadcastDuration: number | undefined;
  duration: number | undefined;
  durationPercentage: number | undefined;
  stage: SQLNodeStageData | SQLNodeExchangeStageData | undefined;
  parsedPlan: ParsedNodePlan | undefined;
  exchangeMetrics: ExchangeMetrics | undefined;
  rddScopeId: string | undefined;
  icebergCommit: IcebergCommitsInfo | undefined;
  deltaLakeScan: DeltaLakeScanInfo | undefined;
  cachedStorage: RddStorageInfo | undefined;
  nodeIdFromMetrics: number | undefined;
}

export interface SQLNodeExchangeStageData {
  type: "exchange";
  writeStage: number;
  readStage: number;
  status: string;
}

export interface SQLNodeStageData {
  type: "onestage";
  stageId: number;
  status: string;
  stageDuration: number;
  restOfStageDuration: number | undefined;
}

export interface EnrichedSqlMetric {
  name: string;
  value: string;
  stageId?: number | undefined;
}

export interface EnrichedSqlEdge {
  fromId: number;
  toId: number;
}

export interface SparkExecutorsStatus {
  numOfExecutors: number;
  totalCoreHour: number;
  totalDriverMemoryGibHour: number;
  totalExecutorMemoryGibHour: number;
  totalMemoryGibHour: number;
  totalDCU: number;
  idleCoresRate: number;
  maxExecutorMemoryPercentage: number;
  maxExecutorMemoryBytesString: string;
  maxExecutorMemoryBytes: number;
  totalInputBytes: string;
  totalShuffleRead: string;
  totalShuffleWrite: string;
}

export interface SparkMetricsStore {
  totalTasks: number;
  executorRunTime: number;
  diskBytesSpilled: number;
  inputBytes: number;
  outputBytes: number;
  shuffleReadBytes: number;
  shuffleWriteBytes: number;
}

export interface SparkSQLResourceUsageStore {
  coreHourUsage: number;
  idleCoresRate: number;
  dcuPercentage: number;
  durationPercentage: number;
  memoryGbHourUsage: number;
  dcu: number;
}

export type SparkJobsStore = SparkJobStore[];

export interface SparkJobStore {
  jobId: number;
  name: string;
  description: string;
  stageIds: number[];
  status: string;

  metrics: SparkMetricsStore;
}

export type SparkStagesStore = SparkStageStore[];

export interface SparkStageStore {
  status: string;
  stageId: number;
  attemptId: number;
  numTasks: number;
  name: string;
  stagesRdd: Record<string, string> | undefined;
  cachedStorage: RddStorageInfo[] | undefined;
  failureReason: string | undefined;
  stageRealTimeDurationMs: number | undefined;
  hasPartitionSkew: boolean | undefined;
  mediumTaskDuration: number | undefined;
  maxTaskDuration: number | undefined;
  completedTasks: number;
  failedTasks: number;
  activeTasks: number;
  pendingTasks: number;
  stageProgress: number;
  durationDistribution: number[];
  inputDistribution: number[];
  outputDistribution: number[];
  shuffleWriteDistribution: number[];
  shuffleReadDistribution: number[];
  inputRowsDistribution: number[];
  spillDiskDistriution: number[];
  outputRowsDistribution: number[];
  metrics: SparkMetricsStore;
}

export type SparkExecutorsStore = SparkExecutorStore[];

export type ExecutorTimelinePoints = ExecutorTimelinePoint[];

export interface ExecutorTimelinePoint {
  timeMs: number;
  value: number;
}

export interface SparkExecutorStore {
  id: string;
  isActive: boolean;
  isDriver: boolean;
  duration: number;
  totalTaskDuration: number;
  potentialTaskTimeMs: number;
  idleCoresRate: number;
  addTimeEpoc: number;
  endTimeEpoc: number;
  totalCores: number;
  maxTasks: number;
  HeapMemoryUsageBytes: number;
  memoryUsagePercentage: number;
  totalInputBytes: number;
  totalShuffleRead: number;
  totalShuffleWrite: number;
}
