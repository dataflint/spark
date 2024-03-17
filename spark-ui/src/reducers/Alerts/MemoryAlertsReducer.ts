import { Alerts, ConfigStore, StatusStore } from "../../interfaces/AppStore";
import { humanFileSizeSparkConfigFormat } from "../../utils/FormatUtils";

const MAX_EXECUTOR_MEMORY_PERCENTAGE_TOO_HIGH_THRESHOLD = 95;
const MAX_EXECUTOR_MEMORY_PERCENTAGE_TOO_LOW_THRESHOLD = 70;
const MEMORY_INCRESE_RATIO = 0.2;
const MEMORY_DECREASE_SAFETRY_BUFFER = 0.2;

export function reduceMemoryAlerts(
  statusStore: StatusStore,
  config: ConfigStore,
  alerts: Alerts,
) {
  if (
    statusStore.executors !== undefined &&
    statusStore.executors.maxExecutorMemoryBytes !== 0
  ) {
    const maxExecutorMemoryPercentage =
      statusStore.executors.maxExecutorMemoryPercentage;
    const maxExecutorMemoryBytes = statusStore.executors.maxExecutorMemoryBytes;
    const maxExecutorMemoryBytesString =
      statusStore.executors?.maxExecutorMemoryBytesString;
    if (
      maxExecutorMemoryPercentage >
      MAX_EXECUTOR_MEMORY_PERCENTAGE_TOO_HIGH_THRESHOLD
    ) {
      const suggestedMemory = humanFileSizeSparkConfigFormat(
        config.executorMemoryBytes * (1 + MEMORY_INCRESE_RATIO),
      );
      alerts.push({
        id: `executorMemoryTooHigh${maxExecutorMemoryPercentage.toFixed(2)}`,
        name: "executorMemoryTooHigh",
        title: "Executor Memory Under-Provisioned",
        location: "In: Summery Page -> Memory Utilization",
        message: `Max Executor Memory usage is ${maxExecutorMemoryPercentage.toFixed(
          2,
        )}% which is too high, and can cause spills and OOMs`,
        suggestion: `
    1. Increase executor memory provisioning by changing "spark.executor.memory" to that value ${suggestedMemory} (the current usage is ${maxExecutorMemoryPercentage.toFixed(
      2,
    )} but set to higher as it needs some buffer) from current value "${
      config.executorMemoryBytesSparkFormatString
    }"`,
        type: "error",
        source: {
          type: "status",
          metric: "memory",
        },
      });
    } else if (
      maxExecutorMemoryPercentage <
      MAX_EXECUTOR_MEMORY_PERCENTAGE_TOO_LOW_THRESHOLD
    ) {
      const suggestedMemory = humanFileSizeSparkConfigFormat(
        maxExecutorMemoryBytes * (1 + MEMORY_DECREASE_SAFETRY_BUFFER),
      );
      alerts.push({
        id: `executorMemoryTooLow_${maxExecutorMemoryPercentage.toFixed(2)}`,
        name: "executorMemoryTooLow",
        title: "Executor Memory Over-Provisioned",
        message: `Max executor memory usage is only ${maxExecutorMemoryPercentage.toFixed(
          2,
        )}%, which means you can provision less memory for each executor and save $$$`,
        location: "In: Summery Page -> Memory Utilization",
        suggestion: `
    1. Decrease each executor memory provisioning by changing "spark.executor.memory" to ${suggestedMemory} (the current usage is ${maxExecutorMemoryPercentage.toFixed(
      2,
    )} but set to higher as it needs some buffer) from current "${
      config.executorMemoryBytesSparkFormatString
    }`,
        type: "warning",
        source: {
          type: "status",
          metric: "memory",
        },
      });
    }
  }
}
