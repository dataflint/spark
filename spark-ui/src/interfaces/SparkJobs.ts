export type SparkJobs = SparkJob[];

export interface SparkJob {
  jobId: number;
  name: string;
  description: string;
  submissionTime: string;
  completionTime: string;
  stageIds: number[];
  status: string;
  numTasks: number;
  numActiveTasks: number;
  numCompletedTasks: number;
  numSkippedTasks: number;
  numFailedTasks: number;
  numKilledTasks: number;
  numCompletedIndices: number;
  numActiveStages: number;
  numCompletedStages: number;
  numSkippedStages: number;
  numFailedStages: number;
  killedTasksSummary: KilledTasksSummary;
  jobGroup?: string;
}

export interface KilledTasksSummary {}
