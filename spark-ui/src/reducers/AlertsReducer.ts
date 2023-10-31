import { v4 as uuidv4 } from 'uuid';
import { Alerts, AlertsStore, ConfigStore, SparkSQLStore, StatusStore } from "../interfaces/AppStore";
import { humanFileSize } from "../utils/FormatUtils";

const MAX_EXECUTOR_MEMORY_PERCENTAGE_TOO_HIGH_THRESHOLD = 95;
const MAX_EXECUTOR_MEMORY_PERCENTAGE_TOO_LOW_THRESHOLD = 70;
const MEMORY_INCRESE_RATIO = 0.2;
const MEMORY_DECREASE_SAFETRY_BUFFER = 0.2;

export function reduceAlers(existingStore: SparkSQLStore,
    statusStore: StatusStore,
    config: ConfigStore
): AlertsStore {
    const alerts: Alerts = [];
    if (statusStore.executors !== undefined && statusStore.executors.maxExecutorMemoryBytes !== 0) {
        const maxExecutorMemoryPercentage = statusStore.executors.maxExecutorMemoryPercentage;
        const maxExecutorMemoryBytes = statusStore.executors.maxExecutorMemoryBytes;
        const maxExecutorMemoryBytesString = statusStore.executors?.maxExecutorMemoryBytesString;
        if (maxExecutorMemoryPercentage > MAX_EXECUTOR_MEMORY_PERCENTAGE_TOO_HIGH_THRESHOLD) {
            const suggestedMemory = humanFileSize(config.executorMemoryBytes * (1 + MEMORY_INCRESE_RATIO));
            alerts.push({
                id: uuidv4(),
                name: 'executorMemoryTooHigh',
                title: 'Executor Memory Under-Provisioned',
                message: `Max Executor Memory usage is ${maxExecutorMemoryPercentage.toFixed(2)}% which is too high, and can cause spills and OOMs`,
                suggestion: `
            1. Increase executor memory provisioning by increasing current provisioning "${config.executorMemoryBytesString}" by ${MEMORY_INCRESE_RATIO * 100}%,
            Increase "spark.executor.memory" from ${config.executorMemoryBytesString} to ${suggestedMemory}
            `,
                type: 'error',
                source: {
                    type: 'status',
                    metric: 'memory'
                }
            });
        } else if (maxExecutorMemoryPercentage < MAX_EXECUTOR_MEMORY_PERCENTAGE_TOO_LOW_THRESHOLD) {
            const suggestedMemory = humanFileSize(maxExecutorMemoryBytes * (1 + MEMORY_DECREASE_SAFETRY_BUFFER));
            alerts.push({
                id: uuidv4(),
                name: 'executorMemoryTooLow',
                title: 'Executor Memory Over-Provisioned',
                message: `Max executor memory usage is only ${maxExecutorMemoryPercentage.toFixed(2)}%, which means you can provision less memory to each executor and save $$$`,
                suggestion: `
            1. Decrease each executor memory provisioning, to the current max executor memory utilization: ${maxExecutorMemoryBytesString} 
            with additional safety bufer of ${MEMORY_DECREASE_SAFETRY_BUFFER * 100}%.
            Decrease "spark.executor.memory" from ${config.executorMemoryBytesString} to ${suggestedMemory}
            `,
                type: 'warning',
                source: {
                    type: 'status',
                    metric: 'memory'
                }
            });
        }
    }

    return {
        alerts: alerts
    }
}
