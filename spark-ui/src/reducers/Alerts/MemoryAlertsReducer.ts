import { Alerts, ConfigStore, StatusStore, SparkExecutorStore } from "../../interfaces/AppStore";
import { humanFileSizeSparkConfigFormat, humanFileSize, calculatePercentage } from "../../utils/FormatUtils";
import { EnvironmentInfo } from "../../interfaces/ApplicationInfo";

const MAX_EXECUTOR_MEMORY_PERCENTAGE_TOO_HIGH_THRESHOLD = 95;
const MAX_EXECUTOR_MEMORY_PERCENTAGE_TOO_LOW_THRESHOLD = 70;
const EXECUTOR_MEMORY_INCREASE_RATIO = 0.2;
const EXECUTOR_MEMORY_DECREASE_SAFETY_BUFFER = 0.2;

const MAX_DRIVER_MEMORY_PERCENTAGE_TOO_HIGH_THRESHOLD = 95;
const DRIVER_MEMORY_INCREASE_RATIO = 0.2;

export function reduceMemoryAlerts(
  { executors: statusExecutors }: StatusStore,
  config: ConfigStore,
  environmentInfo: EnvironmentInfo | undefined, 
  executors: SparkExecutorStore[],
  alerts: Alerts
) {
  if (statusExecutors?.maxExecutorMemoryBytes !== undefined && 
      statusExecutors.maxExecutorMemoryBytes !== 0) {
    reduceExecutorMemoryAlerts(statusExecutors, config, alerts);
  }
  if (environmentInfo?.driverXmxBytes) {
    reduceDriverMemoryAlerts(executors, config, environmentInfo, alerts);
  }
}

function reduceExecutorMemoryAlerts(
  executors: StatusStore["executors"],
  config: ConfigStore,
  alerts: Alerts
) {
  const maxExecutorMemoryPercentage = executors!.maxExecutorMemoryPercentage;
  const maxExecutorMemoryBytes = executors!.maxExecutorMemoryBytes;
  const maxExecutorMemoryBytesString = executors!.maxExecutorMemoryBytesString;

  if (maxExecutorMemoryPercentage > MAX_EXECUTOR_MEMORY_PERCENTAGE_TOO_HIGH_THRESHOLD) {
    const suggestedMemory = humanFileSizeSparkConfigFormat(
      config.executorMemoryBytes * (1 + EXECUTOR_MEMORY_INCREASE_RATIO)
    );
    alerts.push({
      id: `executorMemoryTooHigh_${maxExecutorMemoryPercentage.toFixed(2)}`,
      name: "executorMemoryTooHigh",
      title: "Executor Memory Under-Provisioned",
      location: "In: Summery Page -> Memory Utilization",
      message: `Max Executor Memory usage is ${maxExecutorMemoryPercentage.toFixed(2)}% which is too high, and can cause spills and OOMs`,
      suggestion: `
        1. Increase executor memory provisioning by changing "spark.executor.memory" to ${suggestedMemory}
           (the current usage is ${maxExecutorMemoryPercentage.toFixed(2)}% but set to higher as it needs some buffer)
           from current value "${config.executorMemoryBytesSparkFormatString}"`,
      type: "error",
      source: {
        type: "status",
        metric: "memory"
      }
    });
  } 
  else if (maxExecutorMemoryPercentage < MAX_EXECUTOR_MEMORY_PERCENTAGE_TOO_LOW_THRESHOLD) {
    const suggestedMemory = humanFileSizeSparkConfigFormat(
      maxExecutorMemoryBytes * (1 + EXECUTOR_MEMORY_DECREASE_SAFETY_BUFFER)
    );
    alerts.push({
      id: `executorMemoryTooLow_${maxExecutorMemoryPercentage.toFixed(2)}`,
      name: "executorMemoryTooLow",
      title: "Executor Memory Over-Provisioned",
      location: "In: Summery Page -> Memory Utilization",
      message: `Max executor memory usage is only ${maxExecutorMemoryPercentage.toFixed(2)}%, which means you can provision less memory and save $$$`,
      suggestion: `
        1. Decrease executor memory provisioning by changing "spark.executor.memory" to ${suggestedMemory}
           (the current usage is ${maxExecutorMemoryPercentage.toFixed(2)}% but set to higher as it needs some buffer)
           from current value "${config.executorMemoryBytesSparkFormatString}"`,
      type: "warning",
      source: {
        type: "status",
        metric: "memory"
      }
    
    });
  }
}

function reduceDriverMemoryAlerts(
  executors: SparkExecutorStore[],
  config: ConfigStore,
  environmentInfo: EnvironmentInfo,
  alerts: Alerts
) {
  const driverExecutor = executors?.find((exec) => exec.id === "driver");
  if (!driverExecutor?.HeapMemoryUsageBytes || !environmentInfo.driverXmxBytes) {
    return;
  }

  const driverMaxMemory = environmentInfo.driverXmxBytes;
  const driverMemoryUsage = driverExecutor.HeapMemoryUsageBytes;
  const driverMemoryUsagePercentage = calculatePercentage(driverMemoryUsage, driverMaxMemory);

  if (driverMemoryUsagePercentage > MAX_DRIVER_MEMORY_PERCENTAGE_TOO_HIGH_THRESHOLD) {
    const suggestedMemory = humanFileSizeSparkConfigFormat(
      driverMaxMemory * (1 + DRIVER_MEMORY_INCREASE_RATIO)
    );
    alerts.push({
      id: `driverMemoryTooHigh_${driverMemoryUsagePercentage.toFixed(2)}`,
      name: "driverMemoryTooHigh",
      title: "Driver Memory Under-Provisioned",
      location: "In: Summery Page -> Memory Utilization",
      message: `Max Driver Memory usage is ${driverMemoryUsagePercentage.toFixed(2)}% which is too high, and can cause spills and OOMs`,
      suggestion: `
        1. Increase driver memory provisioning by changing "spark.driver.memory" to ${suggestedMemory}
           (the current usage is ${driverMemoryUsagePercentage.toFixed(2)}% but set to higher as it needs some buffer)
           from current value "${humanFileSizeSparkConfigFormat(driverMaxMemory)}"`,
      type: "error",
      source: {
        type: "status",
        metric: "driverMemory"
      }
    });
  }
}
