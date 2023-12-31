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
}

export interface KilledTasksSummary {}

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
