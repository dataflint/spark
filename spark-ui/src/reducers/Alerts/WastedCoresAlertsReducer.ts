import { Alerts, ConfigStore, StatusStore } from "../../interfaces/AppStore";

const WASTED_CORES_RATIO_THRESHOLD = 50.0;

export function reduceWastedCoresAlerts(
  statusStore: StatusStore,
  config: ConfigStore,
  alerts: Alerts,
) {
  if (
    statusStore.executors !== undefined &&
    statusStore.executors.idleCoresRate > WASTED_CORES_RATIO_THRESHOLD
  ) {
    const idleCores = statusStore.executors.idleCoresRate;

    let suggestionMessage = "decrease amount of cores or executors";
    if (config.resourceControlType === "databricks") {
      suggestionMessage =
        "Reduce your cluster size or machine type via databricks cluster UI";
    } else if (config.resourceControlType === "static") {
      suggestionMessage = `1. decrease amount of cores per executor by lowering spark.executor.cores
      2. decrease amount of executors by lowering spark.executor.instances OR if using dynamic allocation by tuning  .`;
    } else if (config.resourceControlType === "dynamic") {
      suggestionMessage = `1. decrease amount of cores per executor by lowering spark.executor.cores
      2. tune your Dynamic Allocation config, specifically lower spark.dynamicAllocation.executorAllocationRatio or increase spark.dynamicAllocation.schedulerBacklogTimeout`;
    }

    alerts.push({
      id: `idleCoresTooHigh${idleCores.toFixed(2)}`,
      name: "idleCoresTooHigh",
      title: "Idle Cores Too High",
      location: "In: Summery Page -> Idle Cores",
      message: `Idle Cores is ${idleCores.toFixed(
        2,
      )}% which is too high, and suggest your cluster is over-provisioned on cores or executors`,
      suggestion: suggestionMessage,
      type: "warning",
      source: {
        type: "status",
        metric: "idleCores",
      },
    });
  }
}
