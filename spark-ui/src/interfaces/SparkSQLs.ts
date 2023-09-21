
export type SparkSQLs = SparkSQL[]

export interface SparkSQL {
    id: string
    status: string
    description: string
    planDescription: string
    submissionTime: string
    duration: number
    runningJobIds: number[]
    successJobIds: number[]
    failedJobIds: number[]
    nodes: SqlNode[]
    edges: SqlEdge[]
}

export interface SqlNode {
    nodeId: number
    nodeName: string
    metrics: SqlMetric[]
    wholeStageCodegenId?: number
}

export interface SqlMetric {
    name: string
    value: string
}

export interface SqlEdge {
    fromId: number
    toId: number
}

export enum SqlStatus {
    Running = "RUNNING",
    Completed = "COMPLETED",
    Failed = "FAILED"
} 