import { RunMetadataStore } from "../interfaces/AppStore";
import { Attempt } from "../interfaces/SparkApplications";
import { SparkConfiguration } from "../interfaces/SparkConfiguration";

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
): [string, Record<string, string>] {
  const sparkPropertiesObj = Object.fromEntries(
    sparkConfiguration.sparkProperties,
  );
  const systemPropertiesObj = Object.fromEntries(
    sparkConfiguration.systemProperties,
  );
  const runtimeObj = sparkConfiguration.runtime;

  const appName = sparkPropertiesObj["spark.app.name"];
  const config = {
    "spark.app.name": sparkPropertiesObj["spark.app.name"],
    "spark.app.id": sparkPropertiesObj["spark.app.id"],
    "sun.java.command": systemPropertiesObj["sun.java.command"],
    "spark.master": sparkPropertiesObj["spark.master"],
    javaVersion: runtimeObj["javaVersion"],
    scalaVersion: runtimeObj["scalaVersion"],
  };
  return [appName, config];
}
