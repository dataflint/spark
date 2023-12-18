import isEqual from "lodash/isEqual";
import * as Moment from "moment";
import { extendMoment } from "moment-range";
import {
  ConfigStore,
  EnrichedSparkSQL,
  SparkExecutorsStore,
  SparkJobsStore,
  SparkMetricsStore,
  SparkSQLResourceUsageStore,
  SparkSQLStore,
  SparkStagesStore,
  StatusStore
} from "../interfaces/AppStore";
import { SparkJobs } from "../interfaces/SparkJobs";
import { SqlStatus } from "../interfaces/SparkSQLs";
import { SparkStages } from "../interfaces/SparkStages";
import { StagesRdd } from "../interfaces/StagesRdd";
import { msToHours } from "../utils/FormatUtils";
import { calculateSqlStage } from "./SQLNodeStageReducer";

const moment = extendMoment(Moment);

interface ResourceUsage {
  coreUsageMs: number;
  coreHour: number;
  memoryHour: number;
  totalDFU: number;
}

export function calculateStagesStore(
  existingStore: SparkStagesStore | undefined,
  stagesRdd: StagesRdd,
  stages: SparkStages,
): SparkStagesStore {
  //  stages with status SKIPPED does not have metrics, or in the case of numTasks has a "wrong" value

  const stagesStore: SparkStagesStore = stages
    .filter((stage) => stage.status !== "SKIPPED")
    .map((stage) => {
      return {
        stageId: stage.stageId,
        name: stage.name,
        status: stage.status,
        numTasks: stage.numTasks,
        failureReason: stage.failureReason,
        stagesRdd: stagesRdd[stage.stageId],
        metrics: {
          executorRunTime: stage.executorRunTime,
          diskBytesSpilled: stage.diskBytesSpilled,
          inputBytes: stage.inputBytes,
          outputBytes: stage.outputBytes,
          shuffleReadBytes: stage.shuffleReadBytes,
          shuffleWriteBytes: stage.shuffleWriteBytes,
          totalTasks: stage.numTasks,
        },
      };
    });
  return stagesStore;
}

function sumMetricStores(metrics: SparkMetricsStore[]): SparkMetricsStore {
  const jobsMetricsStore: SparkMetricsStore = {
    executorRunTime: metrics
      .map((metrics) => metrics.executorRunTime)
      .reduce((a, b) => a + b, 0),
    diskBytesSpilled: metrics
      .map((metrics) => metrics.diskBytesSpilled)
      .reduce((a, b) => a + b, 0),
    inputBytes: metrics
      .map((metrics) => metrics.inputBytes)
      .reduce((a, b) => a + b, 0),
    outputBytes: metrics
      .map((metrics) => metrics.outputBytes)
      .reduce((a, b) => a + b, 0),
    shuffleReadBytes: metrics
      .map((metrics) => metrics.shuffleReadBytes)
      .reduce((a, b) => a + b, 0),
    shuffleWriteBytes: metrics
      .map((metrics) => metrics.shuffleWriteBytes)
      .reduce((a, b) => a + b, 0),
    totalTasks: metrics
      .map((metrics) => metrics.totalTasks)
      .reduce((a, b) => a + b, 0),
  };
  return jobsMetricsStore;
}

export function calculateJobsMetrics(
  stagesIds: number[],
  stageMetrics: SparkStagesStore,
): SparkMetricsStore {
  const jobsMetrics = stageMetrics
    .filter((stage) => stagesIds.includes(stage.stageId))
    .map((stage) => stage.metrics);
  return sumMetricStores(jobsMetrics);
}

export function calculateJobsStore(
  existingStore: SparkJobsStore | undefined,
  stageMetrics: SparkStagesStore,
  jobs: SparkJobs,
): SparkJobsStore {
  const jobsStore: SparkJobsStore = jobs.map((job) => {
    return {
      jobId: job.jobId,
      name: job.name,
      description: job.description,
      status: job.status,
      stageIds: job.stageIds,
      metrics: calculateJobsMetrics(job.stageIds, stageMetrics),
    };
  });
  return jobsStore;
}

function calculateSqlQueryResourceUsage(
  configStore: ConfigStore,
  sql: EnrichedSparkSQL,
  executors: SparkExecutorsStore,
): ResourceUsage {
  const queryEndTimeEpoc = sql.submissionTimeEpoc + sql.duration;
  const queryRange = moment.range(
    moment(sql.submissionTimeEpoc),
    moment(queryEndTimeEpoc),
  );
  const intersectsAndExecutors = executors.map((executor) => {
    const executorRange = moment.range(
      moment(executor.addTimeEpoc),
      moment(executor.endTimeEpoc),
    );
    const intersect = executorRange.intersect(queryRange);
    return {
      executorId: executor.id,
      isIntersect: intersect !== null,
      intersectRange: intersect,
      intersectTime: intersect === null ? 0 : intersect.valueOf(),
      cores: executor.totalCores,
      memoryGB: (executor.id === "driver" ? configStore.driverMemoryBytes : configStore.executorContainerMemoryBytes) / 1024 / 1024 / 1024
    };
  });
  const intersectTime = intersectsAndExecutors
    .map((intersect) => {
      const coreUsageMs = intersect.intersectTime * intersect.cores;
      const coreHour = msToHours(coreUsageMs);
      const memoryHour = msToHours(intersect.intersectTime * intersect.memoryGB);
      // see documentation about DFU calculation
      const totalDFU = (coreHour * 0.052624) + (memoryHour * 0.0057785)
      return {
        coreUsageMs,
        coreHour,
        memoryHour,
        totalDFU
      };
    })
    .reduce((a, b) => {
      return {
        coreUsageMs: a.coreUsageMs + b.coreUsageMs,
        coreHour: a.coreHour + b.coreHour,
        memoryHour: a.memoryHour + b.memoryHour,
        totalDFU: a.totalDFU + b.totalDFU,
      };
    },
      {
        coreUsageMs: 0,
        coreHour: 0,
        memoryHour: 0,
        totalDFU: 0
      });
  return intersectTime;
}


export function calculateSqlQueryLevelMetricsReducer(
  configStore: ConfigStore,
  existingStore: SparkSQLStore,
  statusStore: StatusStore,
  jobs: SparkJobsStore,
  stages: SparkStagesStore,
  executors: SparkExecutorsStore,
): SparkSQLStore {
  const newSqls = existingStore.sqls
    .map((sql) => {
      const allJobsIds = sql.successJobIds
        .concat(sql.failedJobIds)
        .concat(sql.runningJobIds);
      const sqlJobs = jobs.filter((job) => allJobsIds.includes(job.jobId));
      const allMetrics = sqlJobs.map((job) => job.metrics);
      const sqlMetric = sumMetricStores(allMetrics);
      if (isEqual(sqlMetric, sql.stageMetrics)) {
        return sql;
      }

      return { ...sql, stageMetrics: sqlMetric };
    })
    .map((sql) => {
      const resourceUsageWithDriver = calculateSqlQueryResourceUsage(
        configStore,
        sql,
        executors,
      );
      const resourceUsageExecutorsOnly =
        executors.length === 1
          ? resourceUsageWithDriver
          : calculateSqlQueryResourceUsage(
            configStore,
            sql,
            executors.filter((executor) => !executor.isDriver),
          );
      const totalTasksTime = sql.stageMetrics?.executorRunTime as number;
      const activityRate =
        resourceUsageExecutorsOnly.coreUsageMs !== 0
          ? Math.min(
            100,
            (totalTasksTime / resourceUsageExecutorsOnly.coreUsageMs) * 100,
          )
          : 0;
      const resourceUsageStore: SparkSQLResourceUsageStore = {
        coreHourUsage: resourceUsageWithDriver.coreHour,
        memoryGbHourUsage: resourceUsageWithDriver.memoryHour,
        dfu: resourceUsageWithDriver.totalDFU,
        activityRate: activityRate,
        dfuPercentage:
          statusStore.executors?.totalDFU === undefined
            ? 0
            : Math.min(
              100,
              (resourceUsageWithDriver.totalDFU / statusStore.executors.totalDFU) * 100,
            ),
        durationPercentage:
          statusStore.duration === undefined
            ? 0
            : Math.min(100, (sql.duration / statusStore.duration) * 100),
      };
      return { ...sql, resourceMetrics: resourceUsageStore };
    })
    .map((sql) => {
      const failedSqlJobs = jobs.filter((job) =>
        sql.failedJobIds.includes(job.jobId),
      );
      const jobsStagesIds = failedSqlJobs.flatMap((job) => job.stageIds);
      const jobsStages = stages.filter((stage) =>
        jobsStagesIds.includes(stage.stageId),
      );
      const failureReason = jobsStages.find(
        (stage) => stage.status === SqlStatus.Failed,
      )?.failureReason;

      return { ...sql, failureReason };
    })
    .map(sql => calculateSqlStage(sql, stages, jobs));
  return { sqls: newSqls };
}
