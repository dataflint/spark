import { SparkExecutorsStore } from "../interfaces/AppStore";
import { SparkExecutors } from "../interfaces/SparkExecutors";
import { timeStrToEpocTime } from "../utils/FormatUtils";

export function calculateSparkExecutorsStore(
  existingStore: SparkExecutorsStore | undefined,
  sparkExecutors: SparkExecutors,
  currentEndDate: number,
): SparkExecutorsStore {
  return sparkExecutors.map((executor) => {
    const addTimeEpoc = timeStrToEpocTime(executor.addTime);
    const endTimeEpoc =
      executor.removeTime !== undefined
        ? timeStrToEpocTime(executor.removeTime)
        : currentEndDate;
    return {
      id: executor.id,
      isActive: executor.isActive,
      isDriver: executor.id === "driver",
      duration: endTimeEpoc - addTimeEpoc,
      totalTaskDuration: executor.totalDuration,
      addTimeEpoc: addTimeEpoc,
      endTimeEpoc: endTimeEpoc,
      totalCores: executor.totalCores,
      maxTasks: executor.maxTasks,
    };
  });
}
