import { EnrichedSparkSQL, SparkExecutorStore, SparkExecutorsStore, SparkJobsStore, SparkMetricsStore, SparkSQLResourceUsageStore, SparkSQLStore, SparkStagesStore, StatusStore } from '../interfaces/AppStore';
import { SparkJobs } from "../interfaces/SparkJobs";
import { SparkStages } from "../interfaces/SparkStages";
import isEqual from 'lodash/isEqual';
import { msToHours } from '../utils/FormatUtils';

export function calculateStagesStore(existingStore: SparkStagesStore | undefined, stages: SparkStages): SparkStagesStore {
    //  stages with status SKIPPED does not have metrics, or in the case of numTasks has a "wrong" value
    const stagesStore: SparkStagesStore = stages.filter((stage) => stage.status != "SKIPPED").map(stage => {
        return {
            stageId: stage.stageId,
            name: stage.name,
            status: stage.status,
            numTasks: stage.numTasks,
            metrics: {
                executorRunTime: stage.executorRunTime,
                diskBytesSpilled: stage.diskBytesSpilled,
                inputBytes: stage.inputBytes,
                outputBytes: stage.outputBytes,
                shuffleReadBytes: stage.shuffleReadBytes,
                shuffleWriteBytes: stage.shuffleWriteBytes,
                totalTasks: stage.numTasks
            }
        }
    })
    return stagesStore;
}

function sumMetricStores(metrics: SparkMetricsStore[]): SparkMetricsStore {
    const jobsMetricsStore: SparkMetricsStore = {
        executorRunTime: metrics.map(metrics => metrics.executorRunTime).reduce((a, b) => a + b, 0),
        diskBytesSpilled: metrics.map(metrics => metrics.diskBytesSpilled).reduce((a, b) => a + b, 0),
        inputBytes: metrics.map(metrics => metrics.inputBytes).reduce((a, b) => a + b, 0),
        outputBytes: metrics.map(metrics => metrics.outputBytes).reduce((a, b) => a + b, 0),
        shuffleReadBytes: metrics.map(metrics => metrics.shuffleReadBytes).reduce((a, b) => a + b, 0),
        shuffleWriteBytes: metrics.map(metrics => metrics.shuffleWriteBytes).reduce((a, b) => a + b, 0),
        totalTasks: metrics.map(metrics => metrics.totalTasks).reduce((a, b) => a + b, 0)
    };
    return jobsMetricsStore;    
}

export function calculateJobsMetrics(stagesIds: number[], stageMetrics: SparkStagesStore): SparkMetricsStore {
    const jobsMetrics = stageMetrics.filter(stage => stagesIds.includes(stage.stageId)).map(stage => stage.metrics);
    return sumMetricStores(jobsMetrics);
}

export function calculateJobsStore(existingStore: SparkJobsStore | undefined, stageMetrics: SparkStagesStore, jobs: SparkJobs): SparkJobsStore {
    const jobsStore: SparkJobsStore = jobs.map(job => {
        return {
            jobId: job.jobId,
            name: job.name,
            description: job.description,
            status: job.status,
            stageIds: job.stageIds,
            metrics: calculateJobsMetrics(job.stageIds, stageMetrics)
        }
    })
    return jobsStore;
}

function calculateSqlQueryResourceUsage(sql: EnrichedSparkSQL, executors: SparkExecutorsStore): number {
    const queryEndTimeEpoc = sql.submissionTimeEpoc + sql.duration;
    const queryResourceUsageMs = executors.map(executor => {
        // if the executor was not active during the query time, skip it
        if(executor.addTimeEpoc > queryEndTimeEpoc || executor.endTimeEpoc < sql.submissionTimeEpoc) {
            return 0;
        }

        // if the executor was active during the query time, calculate the time it was active
        const executorStartTime = Math.max(executor.addTimeEpoc, sql.submissionTimeEpoc);
        const executorEndTime = Math.min(executor.endTimeEpoc, queryEndTimeEpoc);
        const executorDuration = executorEndTime - executorStartTime;
        return executorDuration * executor.totalCores;
    }).reduce((a, b) => a + b, 0);
    return queryResourceUsageMs;
}

export function calculateSqlQueryLevelMetrics(existingStore: SparkSQLStore, statusStore: StatusStore, jobs: SparkJobsStore, executors: SparkExecutorsStore): SparkSQLStore {
    const newSqls = existingStore.sqls
    .map(sql => {
        const allJobsIds = sql.successJobIds.concat(sql.failedJobIds).concat(sql.runningJobIds);
        const sqlJobs = jobs.filter(job => allJobsIds.includes(job.jobId));
        const allMetrics = sqlJobs.map(job => job.metrics);
        const sqlMetric = sumMetricStores(allMetrics);
        if(isEqual(sqlMetric, sql.stageMetrics)) {
            return sql;
        }

        return {...sql, stageMetrics: sqlMetric};

    }).map(sql => {
        const queryResourceUsageMs = calculateSqlQueryResourceUsage(sql, executors);
        const totalTasksTime = sql.stageMetrics?.executorRunTime as number;
        const activityRate = totalTasksTime !== 0 ? Math.min(100, (totalTasksTime / queryResourceUsageMs * 100)) : 0;
        const coreHourUsage = msToHours(queryResourceUsageMs);
        const resourceUsageStore: SparkSQLResourceUsageStore = {
            coreHourUsage: coreHourUsage,
            activityRate: activityRate,
            coreHourPercentage: statusStore.executors?.totalCoreHour === undefined ? 0 : Math.min(100, (coreHourUsage / statusStore.executors.totalCoreHour) * 100),
            durationPercentage: statusStore.duration === undefined ? 0 : Math.min(100, (sql.duration / statusStore.duration) * 100)
        }
        return {...sql, resourceMetrics: resourceUsageStore};
    })
    return {sqls: newSqls};
}
