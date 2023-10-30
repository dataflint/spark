import { SparkExecutorsStore } from "../interfaces/AppStore";
import { SparkExecutors } from "../interfaces/SparkExecutors";
import { timeStrToEpocTime } from "../utils/FormatUtils";

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
    const memoryUsagePercentage =
      executorMemoryBytes !== 0
        ? (memoryUsageBytes / executorMemoryBytes) * 100
        : 0;
    return {
      id: executor.id,
      isActive: executor.isActive,
      isDriver: isDriver,
      duration: endTimeEpoc - addTimeEpoc,
      totalTaskDuration: executor.totalDuration,
      addTimeEpoc: addTimeEpoc,
      endTimeEpoc: endTimeEpoc,
      totalCores: executor.totalCores,
      maxTasks: executor.maxTasks,
      memoryUsageBytes: memoryUsageBytes,
      memoryUsagePercentage: memoryUsagePercentage,
    };
  });
}
