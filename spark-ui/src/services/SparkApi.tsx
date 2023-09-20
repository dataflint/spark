import { ApiAction } from "../interfaces/APIAction";
import { AppStore } from "../interfaces/AppStore";
import { SparkApplications } from "../interfaces/SparkApplications";
import { SparkConfiguration } from "../interfaces/SparkConfiguration";
import { SparkSQLs } from '../interfaces/SparkSQLs';
import { SparkStages } from "../interfaces/SparkStages";
import { NodesMetrics } from '../interfaces/SqlMetrics';

const POLL_TIME = 1000

class SparkAPI {
    basePath: string
    initialized: boolean = false;
    appId: string = "";
    apiPath: string;
    applicationsPath: string;
    setStore: React.Dispatch<ApiAction>;
    getStore: () => AppStore;

    private get store(): AppStore {
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

    private getSqlmetricsPath(sqlId: string): string {
        return `${this.basePath}/devtool/sqlmetrics/json/?executionId=${sqlId}`
    }

    

    constructor(basePath: string, setStore: React.Dispatch<ApiAction>, getStore: () => AppStore) {
        this.basePath = basePath;
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
        if (document.hidden) {
            // skip fetching when tab is not in focus
            // TODO: skip also the interval when tab is not in focus
            return;
        }
        try {
            if(!this.initialized) {
                this.initialized = true;
                const appData: SparkApplications = await (await fetch(this.applicationsPath)).json();
                this.appId = appData[0].id;
                const generalConfigParsed = appData[0].attempts[0];
                const currentSparkVersion = generalConfigParsed.appSparkVersion;

                const sparkConfiguration: SparkConfiguration = await (await fetch(this.getEnvironmentPath(this.appId))).json();
                this.setStore({type: 'setInitial', config: sparkConfiguration, appId: this.appId, sparkVersion: currentSparkVersion });
            }
      
            const sparkStages: SparkStages = await (await fetch(this.getStagesPath(this.appId))).json();
            this.setStore({type: 'setStatus', value: sparkStages });

            const sparkSQL: SparkSQLs = await (await fetch(this.getSqlPath(this.appId))).json();

            this.setStore({type: 'setSQL', value: sparkSQL });

            const runningSqlIds = sparkSQL.filter(sql => sql.status === 'RUNNING').map(sql => sql.id)
            if(runningSqlIds.length !== 0) {
                const sqlId = runningSqlIds[0];
                const nodesMetrics: NodesMetrics = await (await fetch(this.getSqlmetricsPath(sqlId))).json();
                this.setStore({type: 'setSQMetrics', value: nodesMetrics, sqlId: sqlId });
            }
          } catch (e) {
            console.log(e);
          }
    }
}

export default SparkAPI;