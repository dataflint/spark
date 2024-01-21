import {
  ConfigEntries,
  ConfigStore,
  ResourceMode,
  RunMetadataStore,
} from "../interfaces/AppStore";
import { Attempt } from "../interfaces/SparkApplications";
import { SparkConfiguration } from "../interfaces/SparkConfiguration";
import { humanFileSize } from "../utils/FormatUtils";

const SIZES: Record<string, number> = {
  b: 1,
  kb: 1024,
  k: 1024,
  mb: Math.pow(1024, 2),
  m: Math.pow(1024, 2),
  gb: Math.pow(1024, 3),
  g: Math.pow(1024, 3),
  t: Math.pow(1024, 4),
  tb: Math.pow(1024, 4),
};

function parseSize(sizeStr: string) {
  const sizeRegex = /^(\d+(\.\d+)?)\s*([a-zA-Z]+)?$/;
  const match = sizeStr.match(sizeRegex);

  if (!match) throw new Error("Invalid size format");

  const size = parseFloat(match[1]);
  const unit = (match[3] || "mb").toLowerCase(); // Default to 'mb' if no unit is provided

  if (unit in SIZES) {
    return size * SIZES[unit];
  } else {
    throw new Error(`Invalid size unit ${unit}`);
  }
}

export function extractRunMetadata(
  name: string,
  appId: string,
  attempt: Attempt,
): RunMetadataStore {
  const endTime =
    attempt.endTime.startsWith('1969-12-31') ? undefined : new Date(attempt.endTime.replaceAll("GMT", "Z")).getTime();

  return {
    appId: appId,
    sparkVersion: attempt.appSparkVersion,
    appName: name,
    startTime: new Date(attempt.startTime).getTime(),
    endTime: endTime,
  };
}

function findResourceControlType(
  sparkConfiguration: Record<string, string>,
): ResourceMode {
  const isDatabricks =
    sparkConfiguration["spark.databricks.clusterUsageTags.cloudProvider"] !==
    undefined;
  const dynamicAllocationEnabled =
    sparkConfiguration["spark.dynamicAllocation.enabled"];
  const staticInstancesExists =
    sparkConfiguration["spark.executor.instances"] !== undefined;
  const masterConfig = sparkConfiguration["spark.master"];
  if (isDatabricks) {
    return "databricks";
  }
  if (masterConfig !== undefined && masterConfig.startsWith("local")) {
    return "local";
  }
  if (
    dynamicAllocationEnabled !== undefined &&
    dynamicAllocationEnabled === "true"
  ) {
    return "dynamic";
  }
  if (staticInstancesExists) {
    return "static";
  }

  return "unknown";
}

export function extractConfig(
  sparkConfiguration: SparkConfiguration,
): [string, ConfigStore] {
  const sparkPropertiesObj = Object.fromEntries(
    sparkConfiguration.sparkProperties,
  );
  const systemPropertiesObj = Object.fromEntries(
    sparkConfiguration.systemProperties,
  );
  const runtimeObj = sparkConfiguration.runtime;

  const executorMemoryStr = sparkPropertiesObj["spark.executor.memory"] ?? "1g";
  const executorMemoryBytes = parseSize(executorMemoryStr);
  const executorMemoryBytesString = humanFileSize(executorMemoryBytes);

  const driverMemoryStr = sparkPropertiesObj["spark.driver.memory"] ?? "1g";
  const driverMemoryBytes = parseSize(driverMemoryStr);
  const driverMemoryBytesString = humanFileSize(driverMemoryBytes);

  const memoryOverheadFactorString =
    sparkPropertiesObj["spark.kubernetes.memoryOverheadFactor"] ??
    sparkPropertiesObj["spark.executor.memoryOverheadFactor"] ??
    "0.1";
  const executorMemoryOverheadFactor = parseFloat(memoryOverheadFactorString);
  const memoryOverheadViaConfigString =
    sparkPropertiesObj["spark.executor.memoryOverhead"] ?? "384m";
  const executorMemoryOverheadViaConfig = parseSize(
    memoryOverheadViaConfigString,
  );
  const totalExectorMemoryViaFactor =
    executorMemoryBytes * executorMemoryOverheadFactor;
  const totalExectorMemoryViaFactorString = humanFileSize(
    totalExectorMemoryViaFactor,
  );
  const executorMemoryOverheadBytes = Math.max(
    totalExectorMemoryViaFactor,
    executorMemoryOverheadViaConfig,
  );
  const executorContainerMemoryBytes =
    executorMemoryBytes + executorMemoryOverheadBytes;
  const executorMemoryOverheadString = humanFileSize(
    executorMemoryOverheadBytes,
  );
  const executorContainerMemoryString = humanFileSize(
    executorContainerMemoryBytes,
  );

  const resourceControlType = findResourceControlType(sparkPropertiesObj);

  const appName = sparkPropertiesObj["spark.app.name"];
  const config: ConfigEntries = [
    {
      name: "app name",
      key: "spark.app.name",
      value: sparkPropertiesObj["spark.app.name"],
      default: undefined,
      documentation:
        "the app name, given by the developer using .appName when creating the session",
      category: "general",
    },
    {
      name: "app id",
      key: "spark.app.id",
      value: sparkPropertiesObj["spark.app.id"],
      default: undefined,
      documentation: "the app id, given internally by spark",
      category: "general",
    },
    {
      name: "java command",
      key: "sun.java.command",
      value: systemPropertiesObj["sun.java.command"],
      default: undefined,
      documentation: "command used to start the session",
      category: "general",
    },
    {
      name: "cluster master address",
      key: "spark.master",
      value: sparkPropertiesObj["spark.master"],
      default: undefined,
      documentation: "the url of the cluster master address",
      category: "general",
    },
    {
      name: "java version",
      key: undefined,
      value: runtimeObj["javaVersion"],
      default: undefined,
      documentation: "java version",
      category: "general",
    },
    {
      name: "scala version",
      key: undefined,
      value: runtimeObj["scalaVersion"],
      default: undefined,
      documentation: "scala version",
      category: "general",
    },
    {
      name: "executor memory overhead via config",
      key: undefined,
      value: memoryOverheadViaConfigString,
      default: undefined,
      documentation: "executor memory overhead via config",
      category: "executor-memory",
    },
    {
      name: "executor memory overhead via factor",
      key: undefined,
      value: totalExectorMemoryViaFactorString,
      default: undefined,
      documentation: "executor memory overhead via factor",
      category: "executor-memory",
    },
    {
      name: "executor memory overhead factor",
      key: undefined,
      value: memoryOverheadFactorString,
      default: undefined,
      documentation: "executor memory overhead factor",
      category: "executor-memory",
    },
    {
      name: "executor memory overhead",
      key: undefined,
      value: executorMemoryOverheadString,
      default: undefined,
      documentation: "executor memory overhead",
      category: "executor-memory",
    },
    {
      name: "executor container memory",
      key: undefined,
      value: executorContainerMemoryString,
      default: undefined,
      documentation: "executor container memory",
      category: "executor-memory",
    },
    {
      name: "executor cores",
      key: "spark.executor.cores",
      value: sparkPropertiesObj["spark.executor.cores"],
      default: "1",
      documentation: "number of core to allocate for each executor",
      category: "resources",
    },
    {
      name: "executor memory",
      key: "spark.executor.memory",
      value: sparkPropertiesObj["spark.executor.memory"],
      default: "1g",
      documentation: "number of memory to allocate for each executor",
      category: "resources",
    },
    {
      name: "driver cores",
      key: "spark.driver.cores",
      value: sparkPropertiesObj["spark.driver.cores"],
      default: "1g",
      documentation: "number of core to allocate for the driver",
      category: "resources",
    },
    {
      name: "driver memory",
      key: "spark.driver.memory",
      value: sparkPropertiesObj["spark.driver.memory"],
      default: "1",
      documentation: "number of memory to allocate for the driver",
      category: "resources",
    },
    {
      name: "executor instances request",
      key: "spark.executor.instances",
      value: sparkPropertiesObj["spark.executor.instances"],
      default: undefined,
      documentation: "number of executor instances",
      category: "static-allocation",
    },
    {
      name: "enabled",
      key: "spark.dynamicAllocation.enabled",
      value: sparkPropertiesObj["spark.dynamicAllocation.enabled"],
      default: "false",
      documentation:
        "Whether to use dynamic resource allocation, which scales the number of executors registered with this application up and down based on the workload",
      category: "dynamic-allocation",
    },
    {
      name: "min executors",
      key: "spark.dynamicAllocation.minExecutors",
      value: sparkPropertiesObj["spark.dynamicAllocation.minExecutors"],
      default: "0",
      documentation:
        "Lower bound for the number of executors if dynamic allocation is enabled",
      category: "dynamic-allocation",
    },
    {
      name: "max executors",
      key: "spark.dynamicAllocation.maxExecutors",
      value: sparkPropertiesObj["spark.dynamicAllocation.maxExecutors"],
      default: "infinity",
      documentation:
        "Upper bound for the number of executors if dynamic allocation is enabled",
      category: "dynamic-allocation",
    },
    {
      name: "scheduler backlog timeout",
      key: "spark.dynamicAllocation.schedulerBacklogTimeout",
      value:
        sparkPropertiesObj["spark.dynamicAllocation.schedulerBacklogTimeout"],
      default: "infinity",
      documentation:
        "If dynamic allocation is enabled and there have been pending tasks backlogged for more than this duration, new executors will be requested",
      category: "dynamic-allocation",
    },
    {
      name: "initial executors",
      key: "spark.dynamicAllocation.initialExecutors",
      value: sparkPropertiesObj["spark.dynamicAllocation.initialExecutors"],
      default:
        sparkPropertiesObj["spark.dynamicAllocation.minExecutors"] ?? "0",
      documentation:
        "Initial number of executors to run if dynamic allocation is enabled.",
      category: "dynamic-allocation-advanced",
    },
    {
      name: "executor allocation ratio",
      key: "spark.dynamicAllocation.executorAllocationRatio",
      value:
        sparkPropertiesObj["spark.dynamicAllocation.executorAllocationRatio"],
      default: "1",
      documentation:
        "This setting allows to set a ratio that will be used to reduce the number of executors w.r.t. full parallelism",
      category: "dynamic-allocation-advanced",
    },
    {
      name: "executor idle timeout",
      key: "spark.dynamicAllocation.executorIdleTimeout",
      value: sparkPropertiesObj["spark.dynamicAllocation.executorIdleTimeout"],
      default: "60s",
      documentation:
        "If dynamic allocation is enabled and an executor has been idle for more than this duration, the executor will be removed",
      category: "dynamic-allocation-advanced",
    },
    {
      name: "cached executor idle timeout",
      key: "spark.dynamicAllocation.cachedExecutorIdleTimeout",
      value:
        sparkPropertiesObj["spark.dynamicAllocation.cachedExecutorIdleTimeout"],
      default: "infinity",
      documentation:
        "If dynamic allocation is enabled and an executor which has cached data blocks has been idle for more than this duration, the executor will be removed",
      category: "dynamic-allocation-super-advanced",
    },
    {
      name: "sustained executor idle timeout",
      key: "spark.dynamicAllocation.sustainedSchedulerBacklogTimeout",
      value:
        sparkPropertiesObj[
        "spark.dynamicAllocation.sustainedSchedulerBacklogTimeout"
        ],
      default: "infinity",
      documentation:
        "If dynamic allocation is enabled and an executor which has cached data blocks has been idle for more than this duration, the executor will be removed",
      category: "dynamic-allocation-super-advanced",
    },
  ];

  return [
    appName,
    {
      resourceControlType: resourceControlType,
      configs: config,
      executorMemoryOverheadViaConfigString: memoryOverheadViaConfigString,
      executorMemoryOverheadFactor: executorMemoryOverheadFactor,
      executorMemoryOverheadViaConfig: executorMemoryOverheadViaConfig,
      executorMemoryOverheadBytes: executorMemoryOverheadBytes,
      executorContainerMemoryBytes: executorContainerMemoryBytes,
      executorMemoryOverheadString: executorMemoryOverheadString,
      executorContainerMemoryString: executorContainerMemoryString,
      executorMemoryBytes: executorMemoryBytes,
      executorMemoryBytesString: executorMemoryBytesString,
      executorMemoryBytesSparkFormatString: executorMemoryStr,
      driverMemoryBytesSparkFormatString: driverMemoryStr,
      driverMemoryBytes,
      driverMemoryBytesString,
    },
  ];
}
