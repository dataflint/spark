import {
  ExecutorTimelinePoint,
  ExecutorTimelinePoints,
  SparkExecutorsStore,
} from "../interfaces/AppStore";
import { SparkExecutors } from "../interfaces/SparkExecutors";
import { calculatePercentage, timeStrToEpocTime } from "../utils/FormatUtils";
import { IS_HISTORY_SERVER_MODE } from "../utils/UrlConsts";

export function calculateSparkExecutorsStore(
  existingStore: SparkExecutorsStore | undefined,
  sparkExecutors: SparkExecutors,
  currentEndDate: number,
  executorMemoryBytes: number,
): SparkExecutorsStore {
  return sparkExecutors.map((executor) => {
    const addTimeEpoc = timeStrToEpocTime(executor.addTime);
    const endTimeEpoc =
      executor.removeTime !== undefined
        ? timeStrToEpocTime(executor.removeTime)
        : currentEndDate;
    const isDriver = executor.id === "driver";
    const memoryUsageBytes =
      (executor.peakMemoryMetrics?.JVMHeapMemory ?? 0) +
      (executor.peakMemoryMetrics?.JVMOffHeapMemory ?? 0);
    const memoryUsagePercentage = calculatePercentage(
      memoryUsageBytes,
      executorMemoryBytes,
    );
    const duration = endTimeEpoc - addTimeEpoc;
    const totalTaskDuration = executor.totalDuration;
    const potentialTaskTimeMs = duration * executor.maxTasks;
    const wastedCoresRate =
      100 - calculatePercentage(totalTaskDuration, potentialTaskTimeMs);
    return {
      id: executor.id,
      isActive: executor.isActive,
      isDriver: isDriver,
      duration: duration,
      totalTaskDuration: totalTaskDuration,
      potentialTaskTimeMs: potentialTaskTimeMs,
      wastedCoresRate: wastedCoresRate,
      addTimeEpoc: addTimeEpoc,
      endTimeEpoc: endTimeEpoc,
      totalCores: executor.totalCores,
      maxTasks: executor.maxTasks,
      memoryUsageBytes: memoryUsageBytes,
      memoryUsagePercentage: memoryUsagePercentage,
      totalInputBytes: executor.totalInputBytes,
      totalShuffleRead: executor.totalShuffleRead,
      totalShuffleWrite: executor.totalShuffleWrite,
    };
  });
}

export function calculateSparkExecutorsTimeline(
  sparkExecutors: SparkExecutorsStore,
  startTimeEpoc: number,
  endTimeEpoc: number,
): ExecutorTimelinePoints {
  const onlyExecutors = sparkExecutors.filter((executor) => !executor.isDriver);

  let resourceEvents: {
    type: "add" | "remove";
    timeMs: number;
    value: number;
  }[] = [];

  onlyExecutors.forEach((executor) => {
    resourceEvents.push({
      type: "add",
      timeMs: executor.addTimeEpoc - startTimeEpoc,
      value: 1,
    });
    if (!executor.isActive || IS_HISTORY_SERVER_MODE) {
      resourceEvents.push({
        type: "remove",
        timeMs: executor.endTimeEpoc - startTimeEpoc,
        value: -1,
      });
    }
  });

  let resourceEventsUnified: {
    type: "add" | "remove";
    timeMs: number;
    value: number;
  }[] = [];

  resourceEvents.forEach((resourceEvent) => {
    const existing = resourceEventsUnified.find(
      (resourceEventUnified) =>
        resourceEvent.type === resourceEventUnified.type &&
        resourceEvent.timeMs === resourceEventUnified.timeMs,
    );
    if (existing === undefined) {
      resourceEventsUnified.push({ ...resourceEvent });
    } else {
      existing.value += resourceEvent.value;
    }
  });

  resourceEventsUnified.sort((a, b) => a.timeMs - b.timeMs);

  let currentExecutorNum = 0;
  const startPoint: ExecutorTimelinePoint = {
    timeMs: 0,
    value: 0,
  };
  const executorTimelinePoints: ExecutorTimelinePoints = [startPoint];

  resourceEventsUnified.forEach((resourceEvent) => {
    currentExecutorNum += resourceEvent.value;
    executorTimelinePoints.push({
      timeMs: resourceEvent.timeMs,
      value: currentExecutorNum,
    });
  });

  if (!IS_HISTORY_SERVER_MODE) {
    executorTimelinePoints.push({
      timeMs: endTimeEpoc - startTimeEpoc,
      value: executorTimelinePoints[executorTimelinePoints.length - 1].value,
    });
  }

  return executorTimelinePoints;
}
