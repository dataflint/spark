export type SparkApplications = SparkApplication[];

export interface SparkApplication {
  id: string;
  name: string;
  attempts: Attempt[];
}

export interface Attempt {
  startTime: string;
  endTime: string;
  lastUpdated: string;
  duration: number;
  sparkUser: string;
  completed: boolean;
  appSparkVersion: string;
  startTimeEpoch: number;
  endTimeEpoch: number;
  lastUpdatedEpoch: number;
}
