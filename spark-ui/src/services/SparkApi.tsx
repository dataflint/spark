import { ApiAction } from "../interfaces/APIAction";
import { AppStore } from "../interfaces/AppStore";
import { SparkApplications } from "../interfaces/SparkApplications";
import { SparkConfiguration } from "../interfaces/SparkConfiguration";
import { SparkSQLs } from '../interfaces/SparkSQLs';
import { SparkStages } from "../interfaces/SparkStages";

const POLL_TIME = 1000

class SparkAPI {
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

    constructor(basePath: string, setStore: React.Dispatch<ApiAction>, getStore: () => AppStore) {
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
            if(!this.initialized) {
                this.initialized = true
                const appData: SparkApplications = await (await fetch(this.applicationsPath)).json();
                this.appId = appData[0].id;
                const generalConfigParsed = appData[0].attempts[0];
                const currentSparkVersion = generalConfigParsed.appSparkVersion;

                const sparkConfiguration: SparkConfiguration = await (await fetch(this.getEnvironmentPath(this.appId))).json();
                this.setStore({type: 'setInitial', config: sparkConfiguration, appId: this.appId, sparkVersion: currentSparkVersion })
            }
      
            const sparkStages: SparkStages = await (await fetch(this.getStagesPath(this.appId))).json()
            this.setStore({type: 'setStatus', value: sparkStages })

            const sparkSQL: SparkSQLs = await (await fetch(this.getSqlPath(this.appId))).json()
            
            this.setStore({type: 'setSQL', value: sparkSQL })
          } catch (e) {
            console.log(e);
          }
    }
}

export default SparkAPI;