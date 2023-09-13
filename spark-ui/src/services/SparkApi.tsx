import { AppStore, StatusStore } from "../interfaces/AppStore";
import { SparkApplications } from "../interfaces/SparkApplications";
import { Runtime, SparkConfiguration } from "../interfaces/SparkConfiguration";
import { SparkSQLs } from '../interfaces/SparkSQLs';
import { SparkStages } from "../interfaces/SparkStages";
import humanFileSize from "../utils/FormatUtils";

const POLL_TIME = 1000

class SparkAPI {
    apiPath: string;
    applicationsPath: string;
    setStore: (store: AppStore) => void;
    getStore: () => AppStore | undefined;

    private get store(): AppStore | undefined {
        return this.getStore()
    }

    private getApplicationPath(appId: string): string {
        return `${this.apiPath}/applications/${appId}`
    }

    private getEnvironmentPath(appId: string): string {
        return `${this.getApplicationPath(appId)}/environment`
    }

    private getSqlPath(appId: string): string {
        return `${this.getApplicationPath(appId)}/sql`
    }

    private getStagesPath(appId: string): string {
        return `${this.getApplicationPath(appId)}/stages`
    }


    constructor(basePath: string, setStore: (store: AppStore) => void, getStore: () => AppStore | undefined) {
        this.apiPath = `${basePath}/api/v1`;
        this.applicationsPath = `${this.apiPath}/applications`;
        this.setStore = setStore
        this.getStore = getStore
    }

    start(): () => void {
        const timerId = setInterval(this.fetchData.bind(this), POLL_TIME)
        return () => clearInterval(timerId)
    }

    async fetchData(): Promise<void> {
        try {
            let currentAppId = "";
            let appName = "";
            let currentSparkVersion = "";
            let config = {};

            if(this.store === undefined) {
                const appData: SparkApplications = await (await fetch(this.applicationsPath)).json();
                currentAppId = appData[0].id;
                const generalConfigParsed = appData[0].attempts[0];
                currentSparkVersion = generalConfigParsed.appSparkVersion;
  
                const sparkConfiguration: SparkConfiguration = await (await fetch(this.getEnvironmentPath(currentAppId))).json();
                const sparkPropertiesObj = Object.fromEntries(sparkConfiguration.sparkProperties);
                const systemPropertiesObj = Object.fromEntries(sparkConfiguration.systemProperties);
                const runtimeObj = sparkConfiguration.runtime;
        
                appName = sparkPropertiesObj["spark.app.name"];
                config = this.extractConfig(sparkPropertiesObj, systemPropertiesObj, runtimeObj, currentSparkVersion);
            } else {
                currentAppId = this.store.appId;
                appName = this.store.appName;
                currentSparkVersion = this.store.sparkVersion;
                config = this.store.config;
            }
      
            const sparkSQL: SparkSQLs = await (await fetch(this.getSqlPath(currentAppId))).json()
            const sparkStages: SparkStages = await (await fetch(this.getStagesPath(currentAppId))).json()
            
            const status = this.calculateStatus(sparkStages)
            this.setStore({
                appId: currentAppId,
                sparkVersion: currentSparkVersion,
                appName: appName,
                config: config,
                sql: sparkSQL,
                status: status
            });
          } catch (e) {
            console.log(e);
          }
    }

    private extractConfig(sparkPropertiesObj: any, systemPropertiesObj: any, runtimeObj: Runtime, currentSparkVersion: string) {
        return {
            "spark.app.name": sparkPropertiesObj["spark.app.name"],
            "spark.app.id": sparkPropertiesObj["spark.app.id"],
            "sun.java.command": systemPropertiesObj["sun.java.command"],
            "spark.master": sparkPropertiesObj["spark.master"],
            "javaVersion": runtimeObj["javaVersion"],
            "scalaVersion": runtimeObj["scalaVersion"],
            "sparkVersion": currentSparkVersion
        };
    }

    private calculateStatus(stages: SparkStages): StatusStore {
        const stagesDataClean = stages.filter((stage: Record<string, any>) =>  stage.status != "SKIPPED")
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
        return state;
    }
}

export default SparkAPI;