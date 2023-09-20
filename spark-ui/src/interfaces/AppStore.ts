export type NodeType = "input" | "output" | "transformation" | "other" | "join";

export interface AppStore {
    isInitialized: boolean
    appId: string | undefined
    sparkVersion: string | undefined
    appName: string | undefined
    status: StatusStore | undefined
    executorsStatus: SparkExecutorsStatus | undefined
    config: Record<string, string> | undefined
    sql: SparkSQLStore | undefined
}

export interface StatusStore {
    totalActiveTasks: number,
    totalPendingTasks: number,
    totalInput: string
    totalOutput: string
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

