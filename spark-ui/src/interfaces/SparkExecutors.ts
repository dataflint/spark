export type SparkExecutors = SparkExecutor[]

export interface SparkExecutor {
  id: string
  hostPort: string
  isActive: boolean
  rddBlocks: number
  memoryUsed: number
  diskUsed: number
  totalCores: number
  maxTasks: number
  activeTasks: number
  failedTasks: number
  completedTasks: number
  totalTasks: number
  totalDuration: number
  totalGCTime: number
  totalInputBytes: number
  totalShuffleRead: number
  totalShuffleWrite: number
  isBlacklisted: boolean
  maxMemory: number
  addTime: string
  executorLogs: ExecutorLogs
  memoryMetrics: MemoryMetrics
  blacklistedInStages: any[]
  peakMemoryMetrics?: PeakMemoryMetrics
  attributes: Attributes
  resources: Resources
  resourceProfileId: number
  isExcluded: boolean
  excludedInStages: any[]
  removeTime: string
  removeReason: string
}

export interface ExecutorLogs {
  stdout?: string
  stderr?: string
}

export interface MemoryMetrics {
  usedOnHeapStorageMemory: number
  usedOffHeapStorageMemory: number
  totalOnHeapStorageMemory: number
  totalOffHeapStorageMemory: number
}

export interface PeakMemoryMetrics {
  JVMHeapMemory: number
  JVMOffHeapMemory: number
  OnHeapExecutionMemory: number
  OffHeapExecutionMemory: number
  OnHeapStorageMemory: number
  OffHeapStorageMemory: number
  OnHeapUnifiedMemory: number
  OffHeapUnifiedMemory: number
  DirectPoolMemory: number
  MappedPoolMemory: number
  ProcessTreeJVMVMemory: number
  ProcessTreeJVMRSSMemory: number
  ProcessTreePythonVMemory: number
  ProcessTreePythonRSSMemory: number
  ProcessTreeOtherVMemory: number
  ProcessTreeOtherRSSMemory: number
  MinorGCCount: number
  MinorGCTime: number
  MajorGCCount: number
  MajorGCTime: number
  TotalGCTime: number
}

export interface Attributes {}

export interface Resources {}
