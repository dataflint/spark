import isEqual from "lodash/isEqual";
import {
  RunMetadataStore,
  SparkExecutorsStatus,
  SparkExecutorsStore,
  StagesSummeryStore,
} from "../interfaces/AppStore";
import { SparkStages } from "../interfaces/SparkStages";
import { humanFileSize, msToHours } from "../utils/FormatUtils";

export function calculateStageStatus(
  existingStore: StagesSummeryStore | undefined,
  stages: SparkStages,
): StagesSummeryStore {
  const stagesDataClean = stages.filter((stage) => stage.status != "SKIPPED");
  const totalActiveTasks = stagesDataClean
    .map((stage) => stage.numActiveTasks)
    .reduce((a, b) => a + b, 0);
  const totalPendingTasks = stagesDataClean
    .map(
      (stage) =>
        stage.numTasks -
        stage.numActiveTasks -
        stage.numFailedTasks -
        stage.numCompleteTasks,
    )
    .reduce((a, b) => a + b, 0);
  const totalInput = stagesDataClean
    .map((stage) => stage.inputBytes)
    .reduce((a, b) => a + b, 0);
  const totalOutput = stagesDataClean
    .map((stage) => stage.outputBytes)
    .reduce((a, b) => a + b, 0);
  const totalShuffleReadBytes = stagesDataClean
    .map((stage) => stage.shuffleReadBytes)
    .reduce((a, b) => a + b, 0);
  const totalShuffleWriteBytes = stagesDataClean
    .map((stage) => stage.shuffleWriteBytes)
    .reduce((a, b) => a + b, 0);
  const totalDiskSpill = stagesDataClean
    .map((stage) => stage.diskBytesSpilled)
    .reduce((a, b) => a + b, 0);
  const totalTaskTimeMs = stagesDataClean
    .map((stage) => stage.executorRunTime)
    .reduce((a, b) => a + b, 0);
  const totalTasks = stagesDataClean
    .map((stage) =>
      stage.numTasks
    )
    .reduce((a, b) => a + b, 0);
  const totalFailedTasks = stagesDataClean
    .map((stage) =>
      stage.numFailedTasks
    )
    .reduce((a, b) => a + b, 0);

  const taskErrorRate = totalTasks !== 0 ? (totalFailedTasks / totalTasks) * 100 : 0;
  const status = totalActiveTasks == 0 ? "idle" : "working";

  const state: StagesSummeryStore = {
    totalActiveTasks: totalActiveTasks,
    totalPendingTasks: totalPendingTasks,
    totalInput: humanFileSize(totalInput),
    totalOutput: humanFileSize(totalOutput),
    totalShuffleRead: humanFileSize(totalShuffleReadBytes),
    totalShuffleWrite: humanFileSize(totalShuffleWriteBytes),
    totalDiskSpill: humanFileSize(totalDiskSpill),
    totalTaskTimeMs: totalTaskTimeMs,
    taskErrorRate: taskErrorRate,
    totalTasks: totalTasks,
    totalFailedTasks: totalFailedTasks,
    status: status,
  };

  if (existingStore === undefined) {
    return state;
  } else if (isEqual(state, existingStore)) {
    return existingStore;
  } else {
    return state;
  }
}

export function calculateSparkExecutorsStatus(
  sparkExecutors: SparkExecutorsStore,
): SparkExecutorsStatus {
  const driver = sparkExecutors.filter((executor) => executor.isDriver)[0];
  const executors = sparkExecutors.filter((executor) => !executor.isDriver);
  const activeExecutors = executors.filter((executor) => executor.isActive);
  const numOfExecutors = activeExecutors.length;

  // if we are in local mode we should only count the driver, if we have executors we should only count the executors
  // because in local mode the driver does the tasks but in cluster mode the executors do the tasks
  const totalTaskTimeMs =
    numOfExecutors === 0
      ? driver.totalTaskDuration
      : executors
        .map((executor) => executor.totalTaskDuration)
        .reduce((a, b) => a + b, 0);
  const totalPotentialTaskTimeMs =
    numOfExecutors === 0
      ? driver.duration * driver.maxTasks
      : executors
        .map((executor) => executor.duration * executor.maxTasks)
        .reduce((a, b) => a + b, 0);
  const totalCoreHour = sparkExecutors
    .map((executor) => executor.totalCores * msToHours(executor.duration))
    .reduce((a, b) => a + b, 0);
  const activityRate =
    totalPotentialTaskTimeMs !== 0 && totalTaskTimeMs !== undefined
      ? Math.min(100, (totalTaskTimeMs / totalPotentialTaskTimeMs) * 100)
      : 0;
  const maxExecutorMemoryPercentage =
    executors.length > 0
      ? Math.max(...executors.map((executor) => executor.memoryUsagePercentage))
      : 0;
  const maxExecutorMemoryBytes =
    executors.length > 0
      ? Math.max(...executors.map((executor) => executor.memoryUsageBytes))
      : 0;
  const maxExecutorMemoryBytesString = humanFileSize(maxExecutorMemoryBytes);
  return {
    numOfExecutors,
    totalCoreHour,
    activityRate,
    maxExecutorMemoryPercentage,
    maxExecutorMemoryBytesString,
  };
}

export function calculateDuration(
  runMetadata: RunMetadataStore,
  currentEpocTime: number,
): number {
  return runMetadata.endTime === undefined
    ? currentEpocTime - runMetadata.startTime
    : runMetadata.endTime - runMetadata.startTime;
}
