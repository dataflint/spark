export type NodeType = "input" | "output" | "transformation" | "other" | "join";

export type AppStore  = {
    isConnected: boolean
    isInitialized: false
    runMetadata: RunMetadataStore | undefined
    config: Record<string, string> | undefined
    status: StatusStore | undefined
    sql: SparkSQLStore | undefined,
    jobs: SparkJobsStore | undefined,
    stages: SparkStagesStore | undefined
} | {
    isConnected: boolean
    isInitialized: true
    runMetadata: RunMetadataStore 
    config: Record<string, string>
    status: StatusStore
    sql: SparkSQLStore | undefined,
    jobs: SparkJobsStore | undefined,
    stages: SparkStagesStore | undefined
}

export interface RunMetadataStore {
    appId: string
    sparkVersion: string
    appName: string
    startTime: number
    endTime: number | undefined
  }

  export interface StatusStore {
    stages: StagesSummeryStore | undefined
    executors: SparkExecutorsStatus | undefined
    duration: number
}


export interface StagesSummeryStore {
    totalActiveTasks: number
    totalPendingTasks: number
    totalInput: string
    totalOutput: string
    totalDiskSpill: string
    status: string
    totalTaskTimeMs: number
  }

  export interface SparkSQLStore {
    sqls: EnrichedSparkSQL[],
  }

  export interface EnrichedSparkSQL {
    id: string
    uniqueId: string
    metricUpdateId: string
    originalNumOfNodes: number
    metrics: SparkMetricsStore

    status: string
    description:string
    planDescription:string
    submissionTime: string
    duration: number
    runningJobIds: number[]
    successJobIds: number[]
    failedJobIds: number[]
    nodes: EnrichedSqlNode[]
    edges: EnrichedSqlEdge[]
}

export interface EnrichedSqlNode {
    nodeId: number
    nodeName: string
    enrichedName: string
    metrics: EnrichedSqlMetric[]
    type: NodeType
    isVisible: boolean
    wholeStageCodegenId? :number
}

export interface EnrichedSqlMetric {
    name: string
    value: string
}

export interface EnrichedSqlEdge{
    fromId: number
    toId: number
}

export interface SparkExecutorsStatus {
    numOfExecutors: number
    totalCoreHour: number
    activityRate: number

}

export interface SparkMetricsStore{
    executorRunTime: number
    diskBytesSpilled: number
    inputBytes: number
    outputBytes: number
    shuffleReadBytes: number
    shuffleWriteBytes: number
}

export type SparkJobsStore = SparkJobStore[]

export interface SparkJobStore{
    jobId: number
    name: string
    description: string
    stageIds: number[]
    status: string

    metrics: SparkMetricsStore
}

export type SparkStagesStore = SparkStageStore[]

export interface SparkStageStore{
    status: string
    stageId: number
    numTasks: number
    name: string

    metrics: SparkMetricsStore
  }
