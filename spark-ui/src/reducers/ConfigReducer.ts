import { ConfigStore, RunMetadataStore } from "../interfaces/AppStore";
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
    attempt.endTimeEpoch === -1 ? undefined : attempt.endTimeEpoch;

  return {
    appId: appId,
    sparkVersion: attempt.appSparkVersion,
    appName: name,
    startTime: attempt.startTimeEpoch,
    endTime: endTime,
  };
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

  const appName = sparkPropertiesObj["spark.app.name"];
  const config: Record<string, string> = {
    "spark.app.name": sparkPropertiesObj["spark.app.name"],
    "spark.app.id": sparkPropertiesObj["spark.app.id"],
    "sun.java.command": systemPropertiesObj["sun.java.command"],
    "spark.master": sparkPropertiesObj["spark.master"],
    javaVersion: runtimeObj["javaVersion"],
    scalaVersion: runtimeObj["scalaVersion"],
    "spark.executor.memory": executorMemoryStr,
    "spark.driver.memory": driverMemoryStr,
    "executor memory overhead via config": memoryOverheadViaConfigString,
    "executor memory overhead via factor": totalExectorMemoryViaFactorString,
    "executor memory overhead factor": memoryOverheadFactorString,
    "executor memory overhead": executorMemoryOverheadString,
    "executor container memory": executorContainerMemoryString,
  };

  return [
    appName,
    {
      rawSparkConfig: config,
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
