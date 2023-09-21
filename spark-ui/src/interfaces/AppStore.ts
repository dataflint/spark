export type NodeType = "input" | "output" | "transformation" | "other" | "join";

export type AppStore  = {
    isInitialized: false
    runMetadata: RunMetadataStore | undefined
    config: Record<string, string> | undefined
    status: StatusStore | undefined
    sql: SparkSQLStore | undefined
} | {
    isInitialized: true
    runMetadata: RunMetadataStore 
    config: Record<string, string>
    status: StatusStore
    sql: SparkSQLStore | undefined
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
    totalActiveTasks: number,
    totalPendingTasks: number,
    totalInput: string
    totalOutput: string
    totalDiskSpill: string
    status: string
  }

  export interface SparkSQLStore {
    sqls: EnrichedSparkSQL[],
  }

  export interface EnrichedSparkSQL {
    id: string
    uniqueId: string
    metricUpdateId: string
    originalNumOfNodes: number

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
    numOfExecutors: number;
}

export interface SparkJobsStore{
}

export interface SparkJobStore{
}

