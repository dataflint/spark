import { SparkSQLs } from "./SparkSQLs"

export interface AppStore {
    appId: string
    sparkVersion: string
    appName: string
    status: StatusStore
    config: Record<string, string>
    sql: SparkSQLs
}

export interface StatusStore {
    totalActiveTasks: number,
    totalPendingTasks: number,
    totalInput: string
    totalOutput: string
    status: string
  }
