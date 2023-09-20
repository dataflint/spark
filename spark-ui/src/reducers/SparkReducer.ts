import { ApiAction } from "../interfaces/APIAction";
import { AppStore, SparkExecutorsStatus, StatusStore } from '../interfaces/AppStore';
import { SparkConfiguration } from "../interfaces/SparkConfiguration";
import { SparkStages } from "../interfaces/SparkStages";
import { humanFileSize } from "../utils/FormatUtils";
import isEqual from 'lodash/isEqual';
import { calculateSqlStore, updateSqlMetrics } from "./SqlReducer";
import { SparkExecutors } from "../interfaces/SparkExecutors";

function extractConfig(sparkConfiguration: SparkConfiguration): [string, Record<string, string>] {
    const sparkPropertiesObj = Object.fromEntries(sparkConfiguration.sparkProperties);
    const systemPropertiesObj = Object.fromEntries(sparkConfiguration.systemProperties);
    const runtimeObj = sparkConfiguration.runtime;

    const appName = sparkPropertiesObj["spark.app.name"];
    const config = {
        "spark.app.name": sparkPropertiesObj["spark.app.name"],
        "spark.app.id": sparkPropertiesObj["spark.app.id"],
        "sun.java.command": systemPropertiesObj["sun.java.command"],
        "spark.master": sparkPropertiesObj["spark.master"],
        "javaVersion": runtimeObj["javaVersion"],
        "scalaVersion": runtimeObj["scalaVersion"]
    };
    return [appName, config]
}

function calculateStatus(existingStore: StatusStore | undefined, stages: SparkStages): StatusStore {
    const stagesDataClean = stages.filter((stage: Record<string, any>) => stage.status != "SKIPPED")
    const totalActiveTasks = stagesDataClean.map((stage: Record<string, any>) => stage.numActiveTasks).reduce((a: number, b: number) => a + b, 0);
    const totalPendingTasks = stagesDataClean.map((stage: Record<string, any>) => stage.numTasks - stage.numActiveTasks - stage.numFailedTasks - stage.numCompleteTasks).reduce((a: number, b: number) => a + b, 0);
    const totalInput = stagesDataClean.map((stage: Record<string, any>) => stage.inputBytes).reduce((a: number, b: number) => a + b, 0);
    const totalOutput = stagesDataClean.map((stage: Record<string, any>) => stage.outputBytes).reduce((a: number, b: number) => a + b, 0);
    const status = totalActiveTasks == 0 ? "idle" : "working";

    const state: StatusStore = {
        totalActiveTasks: totalActiveTasks,
        totalPendingTasks: totalPendingTasks,
        totalInput: humanFileSize(totalInput),
        totalOutput: humanFileSize(totalOutput),
        status: status
    }

    if(existingStore === undefined) {
        return state;
    } else if(isEqual(state, existingStore)) {
        return existingStore;
    } else {
        return state;
    }
}

function calculateSparkExecutorsStatus(existingStore: SparkExecutorsStatus | undefined, sparkExecutors: SparkExecutors): SparkExecutorsStatus {
    const driver = sparkExecutors.filter(executor => executor.id === "driver")[0];
    const executors = sparkExecutors.filter(executor => executor.id !== "driver");
    const numOfExecutors = executors.length;
    const availableExecutorCores = numOfExecutors !== 0 ? executors.map(executor => executor.totalCores).reduce((a, b) => a + b, 0) : 0;
    const driverMemoryUtilizationPrecentage = driver.maxMemory !== 0 ? (driver.memoryUsed / driver.maxMemory) * 100 : 0;
    const maxExecutorsMemoryUtilizationPrecentage = numOfExecutors !== 0 ? Math.max(...executors.map(executor => executor.maxMemory !== 0 ? (executor.memoryUsed / executor.maxMemory) * 100 : 0)) : 0;
    const state = {
        numOfExecutors,
        driverMemoryUtilizationPrecentage,
        maxExecutorsMemoryUtilizationPrecentage,
        availableExecutorCores
    }

    if(existingStore === undefined) {
        return state;
    } else if(isEqual(state, existingStore)) {
        return existingStore;
    } else {
        return state;
    }
}


export function sparkApiReducer(store: AppStore, action: ApiAction): AppStore {
    switch (action.type) {
        case 'setInitial':
            const [appName, config] = extractConfig(action.config)
            return { ...store, isInitialized: true, appName: appName, appId: action.appId, sparkVersion: action.sparkVersion, config: config, status: undefined, sql: undefined };
        case 'setSQL':
            const sqlStore = calculateSqlStore(store.sql, action.value);
            if(sqlStore === store.sql) {
                return store;
            } else {
                return { ...store, sql: sqlStore };
            }
        case 'setStatus':
            const status = calculateStatus(store.status, action.value);
            if(status === store.status) {
                return store;
            } else {
                return { ...store, status: status };
            }
            case 'setSparkExecutors':
                const executorsStatus = calculateSparkExecutorsStatus(store.executorsStatus, action.value);
                if(executorsStatus === store.executorsStatus) {
                    return store;
                } else {
                    return { ...store, executorsStatus: executorsStatus };
                }
        case 'setSQMetrics':
            if(store.sql === undefined) {
                // Shouldn't happen as store should be initialized when we get updated metrics
                return store;
            } else {
                return {...store, sql: updateSqlMetrics(store.sql, action.sqlId, action.value) };
            }
        default:
            return store;
    }
}
