import { EnrichedSparkSQL, SparkExecutorStore, SparkExecutorsStore, SparkJobsStore, SparkMetricsStore, SparkSQLResourceUsageStore, SparkSQLStore, SparkStagesStore, StatusStore } from '../interfaces/AppStore';
import { SparkJobs } from "../interfaces/SparkJobs";
import { SparkStages } from "../interfaces/SparkStages";
import isEqual from 'lodash/isEqual';
import { msToHours } from '../utils/FormatUtils';
import * as Moment from 'moment';
import { extendMoment } from 'moment-range';

const moment = extendMoment(Moment);

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
    const queryRange = moment.range(moment(sql.submissionTimeEpoc), moment(queryEndTimeEpoc));
    const intersectsAndExecutors = executors.map(executor => {
        const executorRange = moment.range(moment(executor.addTimeEpoc), moment(executor.endTimeEpoc));
        const intersect = executorRange.intersect(queryRange);
        return {
            executorId: executor.id,
            isIntersect: intersect !== null,
            intersectRange: intersect,
            intersectTime: intersect === null ? 0 : intersect.valueOf(),
            cores: executor.totalCores

        };
    });
    const intersectTime = intersectsAndExecutors.map(intersect => intersect.intersectTime * intersect.cores).reduce((a, b) => a + b, 0);
    return intersectTime;
}

export function calculateSqlQueryLevelMetricsReducer(existingStore: SparkSQLStore, statusStore: StatusStore, jobs: SparkJobsStore, executors: SparkExecutorsStore): SparkSQLStore {
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
        const queryResourceUsageWithDriverMs = calculateSqlQueryResourceUsage(sql, executors);
        const queryResourceUsageExecutorsOnlyMs = executors.length === 1 ? queryResourceUsageWithDriverMs : calculateSqlQueryResourceUsage(sql, executors.filter(executor => !executor.isDriver));
        const totalTasksTime = sql.stageMetrics?.executorRunTime as number;
        const activityRate = queryResourceUsageExecutorsOnlyMs !== 0 ? Math.min(100, (totalTasksTime / queryResourceUsageExecutorsOnlyMs) * 100) : 0;
        const coreHourUsage = msToHours(queryResourceUsageWithDriverMs);
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
