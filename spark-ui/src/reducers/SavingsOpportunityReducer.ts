import {
  Alerts,
  SavingsOpportunity,
  SparkSQLStore,
  StatusStore,
} from "../interfaces/AppStore";

// Rough share of a query's runtime (and so of its compute cost, as executors are held for the
// whole query) that fixing the alert removes. partitionSkew, smallTasks, broadcastSmallTable and
// readSmallFiles are calibrated on before/after runs (see SavingsFixDemo in example_3_5_1) and
// rounded down; the rest are heuristics. This is the OSS estimate, not the DataFlint engine's model.
const RUNTIME_REDUCTION_BY_ALERT: { [alertName: string]: number } = {
  partitionSkew: 0.6,
  smallTasks: 0.6,
  largeCrossJoinScan: 0.5,
  fullScanPartitionedTable: 0.4,
  fullScanZIndexTable: 0.4,
  fullScanLiquidClusteringTable: 0.4,
  broadcastSmallTable: 0.35,
  inefficientIcebergReplaceTable: 0.3,
  replacedMostOfIcebergTable: 0.3,
  maxPartitionToBig: 0.2,
  readSmallFiles: 0.1,
  largeBroadcast: 0.1,
  longFilterCondition: 0.1,
};

// Alerts whose fix mostly gives back cores that sat idle (e.g. waiting on a skewed task), so their
// savings come out of the idle cores pool rather than adding to it
const IDLE_RECLAIMING_ALERTS = new Set(["partitionSkew"]);

// Share of unused executor memory that can be trimmed while keeping a safety buffer
const RECOVERABLE_MEMORY_HEADROOM_SHARE = 0.5;

// Same weights as the DCU calculation (see StatusReducer)
const CORE_HOUR_DCU = 0.05;
const MEMORY_GIB_HOUR_DCU = 0.005;

/**
 * Estimates how much of the app's compute cost (in DCU) could be saved, and attaches a
 * per-alert estimate as `savingsPercentage` on the alerts it can price.
 *
 * Query fixes come first: each alerted query saves its DCU times the alerts' runtime reduction,
 * where several alerts on the same query compound (1 - (1 - a) * (1 - b) ...).
 * Idle cores and over-provisioned memory are then right-sized on the cost that is left. Fixes that
 * reclaim idle cores (skew) are taken out of the idle pool, so that idle time isn't counted twice.
 */
export function reduceSavingsOpportunity(
  sqlStore: SparkSQLStore,
  statusStore: StatusStore,
  alerts: Alerts,
): SavingsOpportunity | undefined {
  const executors = statusStore.executors;
  if (executors === undefined || executors.totalDCU <= 0) {
    return undefined;
  }
  const totalDCU = executors.totalDCU;
  const toPercentage = (dcu: number) => (dcu / totalDCU) * 100;

  const sqlsById = new Map(sqlStore.sqls.map((sql) => [sql.id, sql]));
  const sqlDCUBySqlId = new Map<string, number>();
  // share of the query's runtime left after all its fixes
  const remainingRuntimeBySqlId = new Map<string, number>();
  const idleReclaimingDCUBySqlId = new Map<string, number>();
  for (const alert of alerts) {
    if (alert.source.type === "status") {
      continue;
    }
    const runtimeReduction = RUNTIME_REDUCTION_BY_ALERT[alert.name];
    const resources = sqlsById.get(alert.source.sqlId)?.resourceMetrics;
    if (runtimeReduction === undefined || resources === undefined) {
      continue;
    }
    const sqlId = alert.source.sqlId;
    const sqlDCU = totalDCU * (resources.dcuPercentage / 100);
    const savingsDCU = sqlDCU * runtimeReduction;
    alert.savingsPercentage = toPercentage(savingsDCU);

    sqlDCUBySqlId.set(sqlId, sqlDCU);
    remainingRuntimeBySqlId.set(
      sqlId,
      (remainingRuntimeBySqlId.get(sqlId) ?? 1) * (1 - runtimeReduction),
    );
    if (IDLE_RECLAIMING_ALERTS.has(alert.name)) {
      idleReclaimingDCUBySqlId.set(
        sqlId,
        Math.max(idleReclaimingDCUBySqlId.get(sqlId) ?? 0, savingsDCU),
      );
    }
  }

  let queriesDCU = 0;
  remainingRuntimeBySqlId.forEach(
    (remainingRuntime, sqlId) =>
      (queriesDCU += (sqlDCUBySqlId.get(sqlId) ?? 0) * (1 - remainingRuntime)),
  );
  queriesDCU = Math.min(queriesDCU, totalDCU);
  let idleReclaimingDCU = 0;
  idleReclaimingDCUBySqlId.forEach((dcu) => (idleReclaimingDCU += dcu));
  const remainingShare = 1 - queriesDCU / totalDCU;

  const coreDCU = executors.totalCoreHour * CORE_HOUR_DCU;
  const appIdleCoresDCU = coreDCU * (executors.idleCoresRate / 100);
  // idle left after the idle-reclaiming fixes, shrunk by the other fixes like the rest of the cost
  const idleCoresDCU =
    Math.max(0, appIdleCoresDCU - idleReclaimingDCU) *
    (1 - (queriesDCU - idleReclaimingDCU) / totalDCU);
  let memoryDCU = 0;

  for (const alert of alerts) {
    if (alert.name === "idleCoresTooHigh") {
      alert.savingsPercentage = toPercentage(idleCoresDCU);
    } else if (alert.name === "executorMemoryTooLow") {
      const unusedMemoryRate = 1 - executors.maxExecutorMemoryPercentage / 100;
      memoryDCU =
        executors.totalExecutorMemoryGibHour *
        MEMORY_GIB_HOUR_DCU *
        unusedMemoryRate *
        RECOVERABLE_MEMORY_HEADROOM_SHARE *
        remainingShare;
      alert.savingsPercentage = toPercentage(memoryDCU);
    }
  }

  const totalSavingsDCU = queriesDCU + idleCoresDCU + memoryDCU;
  return {
    totalPercentage: Math.min(100, toPercentage(totalSavingsDCU)),
    idleCoresPercentage: toPercentage(idleCoresDCU),
    queriesPercentage: toPercentage(queriesDCU),
    memoryPercentage: toPercentage(memoryDCU),
  };
}
