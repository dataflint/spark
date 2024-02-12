export type SparkStages = SparkStage[];

export interface SparkStage {
  status: string;
  stageId: number;
  attemptId: number;
  numTasks: number;
  numActiveTasks: number;
  numCompleteTasks: number;
  numFailedTasks: number;
  numKilledTasks: number;
  numCompletedIndices: number;
  submissionTime?: string;
  firstTaskLaunchedTime?: string;
  completionTime?: string;
  executorDeserializeTime: number;
  executorDeserializeCpuTime: number;
  executorRunTime: number;
  executorCpuTime: number;
  resultSize: number;
  jvmGcTime: number;
  resultSerializationTime: number;
  memoryBytesSpilled: number;
  diskBytesSpilled: number;
  peakExecutionMemory: number;
  inputBytes: number;
  inputRecords: number;
  outputBytes: number;
  outputRecords: number;
  shuffleRemoteBlocksFetched: number;
  shuffleLocalBlocksFetched: number;
  shuffleFetchWaitTime: number;
  shuffleRemoteBytesRead: number;
  shuffleRemoteBytesReadToDisk: number;
  shuffleLocalBytesRead: number;
  shuffleReadBytes: number;
  shuffleReadRecords: number;
  shuffleCorruptMergedBlockChunks: number;
  shuffleMergedFetchFallbackCount: number;
  shuffleMergedRemoteBlocksFetched: number;
  shuffleMergedLocalBlocksFetched: number;
  shuffleMergedRemoteChunksFetched: number;
  shuffleMergedLocalChunksFetched: number;
  shuffleMergedRemoteBytesRead: number;
  shuffleMergedLocalBytesRead: number;
  shuffleRemoteReqsDuration: number;
  shuffleMergedRemoteReqsDuration: number;
  shuffleWriteBytes: number;
  shuffleWriteTime: number;
  shuffleWriteRecords: number;
  name: string;
  details: string;
  schedulingPool: string;
  rddIds: number[];
  accumulatorUpdates: any[];
  killedTasksSummary: KilledTasksSummary;
  resourceProfileId: number;
  peakExecutorMetrics?: PeakExecutorMetrics;
  isShufflePushEnabled: boolean;
  shuffleMergersCount: number;
  failureReason: string | undefined;
  taskMetricsDistributions: TaskMetricsDistributions | undefined
}

export interface KilledTasksSummary { }

export interface PeakExecutorMetrics {
  JVMHeapMemory: number;
  JVMOffHeapMemory: number;
  OnHeapExecutionMemory: number;
  OffHeapExecutionMemory: number;
  OnHeapStorageMemory: number;
  OffHeapStorageMemory: number;
  OnHeapUnifiedMemory: number;
  OffHeapUnifiedMemory: number;
  DirectPoolMemory: number;
  MappedPoolMemory: number;
  ProcessTreeJVMVMemory: number;
  ProcessTreeJVMRSSMemory: number;
  ProcessTreePythonVMemory: number;
  ProcessTreePythonRSSMemory: number;
  ProcessTreeOtherVMemory: number;
  ProcessTreeOtherRSSMemory: number;
  MinorGCCount: number;
  MinorGCTime: number;
  MajorGCCount: number;
  MajorGCTime: number;
  TotalGCTime: number;
}

export interface TaskMetricsDistributions {
  quantiles: number[]
  duration: number[]
  executorDeserializeTime: number[]
  executorDeserializeCpuTime: number[]
  executorRunTime: number[]
  executorCpuTime: number[]
  resultSize: number[]
  jvmGcTime: number[]
  resultSerializationTime: number[]
  gettingResultTime: number[]
  schedulerDelay: number[]
  peakExecutionMemory: number[]
  memoryBytesSpilled: number[]
  diskBytesSpilled: number[]
  inputMetrics: InputMetrics
  outputMetrics: OutputMetrics
  shuffleReadMetrics: ShuffleReadMetrics
}

export interface InputMetrics {
  bytesRead: number[]
  recordsRead: number[]
}

export interface OutputMetrics {
  bytesWritten: number[]
  recordsWritten: number[]
}

export interface ShuffleReadMetrics {
  readBytes: number[]
  readRecords: number[]
  remoteBlocksFetched: number[]
  localBlocksFetched: number[]
  fetchWaitTime: number[]
  remoteBytesRead: number[]
  remoteBytesReadToDisk: number[]
  totalBlocksFetched: number[]
  remoteReqsDuration: number[]
  shufflePushReadMetricsDist: ShufflePushReadMetricsDist
}

export interface ShufflePushReadMetricsDist {
  corruptMergedBlockChunks: number[]
  mergedFetchFallbackCount: number[]
  remoteMergedBlocksFetched: number[]
  localMergedBlocksFetched: number[]
  remoteMergedChunksFetched: number[]
  localMergedChunksFetched: number[]
  remoteMergedBytesRead: number[]
  localMergedBytesRead: number[]
  remoteMergedReqsDuration: number[]
}
