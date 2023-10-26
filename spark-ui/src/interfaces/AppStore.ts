export type NodeType = "input" | "output" | "transformation" | "other" | "join";

export type AppStore = {
  isConnected: boolean;
  isInitialized: boolean;
  runMetadata: RunMetadataStore | undefined;
  config: Record<string, string> | undefined;
  status: StatusStore | undefined;
  sql: SparkSQLStore | undefined;
  jobs: SparkJobsStore | undefined;
  stages: SparkStagesStore | undefined;
  executors: SparkExecutorsStore | undefined;
};

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
  duration: number;
}

export interface StagesSummeryStore {
  totalActiveTasks: number;
  totalPendingTasks: number;
  totalInput: string;
  totalOutput: string;
  totalDiskSpill: string;
  status: string;
  totalTaskTimeMs: number;
}

export interface SparkSQLStore {
  sqls: EnrichedSparkSQL[];
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
  edges: EnrichedSqlEdge[];
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

export type ParsedWriteToHDFSPlan = {
  location: string;
  format: string;
  mode: string;
  tableName?: string;
  partitionKeys?: string[];
};

export type ParsedCollectLimitPlan = {
  limit: number;
};

export type ParsedNodePlan =
  | { type: "HashAggregate"; plan: ParsedHashAggregatePlan }
  | { type: "TakeOrderedAndProject"; plan: ParsedTakeOrderedAndProjectPlan }
  | { type: "CollectLimit"; plan: ParsedCollectLimitPlan }
  | { type: "FileScan"; plan: ParseFileScanPlan }
  | { type: "WriteToHDFS"; plan: ParsedWriteToHDFSPlan };

export interface EnrichedSqlNode {
  nodeId: number;
  nodeName: string;
  enrichedName: string;
  metrics: EnrichedSqlMetric[];
  type: NodeType;
  isVisible: boolean;
  wholeStageCodegenId?: number;
  parsedPlan: ParsedNodePlan | undefined;
}

export interface EnrichedSqlMetric {
  name: string;
  value: string;
}

export interface EnrichedSqlEdge {
  fromId: number;
  toId: number;
}

export interface SparkExecutorsStatus {
  numOfExecutors: number;
  totalCoreHour: number;
  activityRate: number;
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
  activityRate: number;
  coreHourPercentage: number;
  durationPercentage: number;
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
  numTasks: number;
  name: string;

  metrics: SparkMetricsStore;
}

export type SparkExecutorsStore = SparkExecutorStore[];

export interface SparkExecutorStore {
  id: string;
  isActive: boolean;
  isDriver: boolean;
  duration: number;
  totalTaskDuration: number;
  addTimeEpoc: number;
  endTimeEpoc: number;
  totalCores: number;
  maxTasks: number;
}
