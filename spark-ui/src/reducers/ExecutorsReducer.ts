import { SparkExecutorsStore } from "../interfaces/AppStore";
import { SparkExecutors } from "../interfaces/SparkExecutors";
import { timeStrToEpocTime } from "../utils/FormatUtils";

export function calculateSparkExecutorsStore(existingStore: SparkExecutorsStore | undefined, sparkExecutors: SparkExecutors): SparkExecutorsStore {
    return sparkExecutors.map(executor => {
        const addTimeEpoc = timeStrToEpocTime(executor.addTime);
        const endTimeEpoc = addTimeEpoc + executor.totalDuration;
        return {
            id: executor.id,
            totalDuration: executor.totalDuration,
            addTimeEpoc: addTimeEpoc,
            endTimeEpoc: endTimeEpoc,
            totalCores: executor.totalCores
        }
    })
}
