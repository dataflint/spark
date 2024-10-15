import { SparkApplication } from "./SparkApplications";

export interface EnvironmentInfo {
  driverXmxBytes?: number;
}

export interface ApplicationInfo {
  runId?: string;
  info: SparkApplication;
  environmentInfo?: EnvironmentInfo; 
}