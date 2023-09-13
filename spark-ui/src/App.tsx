import * as React from 'react';
import Box from '@mui/material/Box';
import AppBar from './AppBar';
import Toolbar from '@mui/material/Toolbar';
import Grid from '@mui/material/Grid';
import InfoBox from './InfoBox';
import ConfigTable from './ConfigTable';
import ApiIcon from '@mui/icons-material/Api';
import ArrowUpwardIcon from '@mui/icons-material/ArrowUpward';
import ArrowDownwardIcon from '@mui/icons-material/ArrowDownward';
import QueueIcon from '@mui/icons-material/Queue';
import API from './services/Api';
import { SqlResponse } from './interfaces/SqlInterfaces';

const WORKING_POLL_TIME = 1000
const IDLE_POLL_TIME = 10000
let BASE_PATH = ""
if(process.env.NODE_ENV === 'development' ) {
  BASE_PATH = "http://localhost:10000";
}
const API_PATH = "api/v1";
const APPLICATION_PATH = "applications";
const APPLICATION_ENCIRONMENT_PATH = "environment";
const STAGES_PATH = "stages";
var POLL_TIME = WORKING_POLL_TIME;
// http://desktop-qeuprph:4040/api/v1/applications/local-1682685376172/environment
// process.env.SPARK_UI_URI

interface StatusProps {
  totalActiveTasks: number,
  totalPendingTasks: number,
  totalInput: string
  totalOutput: string
  status: string
}

function humanFileSize(bytes: number, si = true): string {
  let thresh = si ? 1000 : 1024;
  if (bytes < thresh) return bytes + ' B';
  let units = si ? ['kB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'] : ['KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'];
  let u = -1;
  do {
    bytes /= thresh;
    ++u;
  } while (bytes >= thresh);
  return bytes.toFixed(1) + ' ' + units[u];
};

export default function App() {
  const [appId, setAppId] = React.useState("");
  const [generalConfig, setGeneralConfig] = React.useState({ sparkVersion: undefined });
  const [config, setConfig] = React.useState({});
  const [appName, setAppName] = React.useState("");
  const [sqlData, setSqlData] = React.useState<SqlResponse | undefined>();


  const [stats, setStats] = React.useState<StatusProps>();

  const fetchSparkData = async () => {
    try {
      let currentAppId = appId
      let currentSparkVersion = generalConfig.sparkVersion
      if (!currentAppId) {
        const app_res = await fetch(`${BASE_PATH}/${API_PATH}/${APPLICATION_PATH}`);
        const app_data = await app_res.json();
        currentAppId = app_data[0].id;
        const generalConfigParsed = app_data[0].attempts[0];
        console.log("Found app id: " + currentAppId);
        currentSparkVersion = generalConfigParsed.appSparkVersion;
        setAppId(currentAppId);
        setGeneralConfig({
          sparkVersion: currentSparkVersion
        });
      }
      const appIdBasePath = `${BASE_PATH}/${API_PATH}/${APPLICATION_PATH}/${currentAppId}`
      if (Object.keys(config).length === 0) {
        const appEnvRes = await fetch(`${appIdBasePath}/${APPLICATION_ENCIRONMENT_PATH}`);
        const appEnvData = await appEnvRes.json();

        const sparkPropertiesObj = Object.fromEntries(appEnvData.sparkProperties);
        const systemPropertiesObj = Object.fromEntries(appEnvData.systemProperties);
        const runtimeObj = appEnvData.runtime;

        sparkPropertiesObj["spark.master"]

        setAppName(sparkPropertiesObj["spark.app.name"]);
        setConfig({
          "spark.app.name": sparkPropertiesObj["spark.app.name"],
          "spark.app.id": sparkPropertiesObj["spark.app.id"],
          "sun.java.command": systemPropertiesObj["sun.java.command"],
          "spark.master": sparkPropertiesObj["spark.master"],
          "javaVersion": runtimeObj["javaVersion"],
          "scalaVersion": runtimeObj["scalaVersion"],
          "sparkVersion": currentSparkVersion
        });

        setSqlData(await API.getSqlData(currentAppId));
      }

      const stagesRes = await fetch(`${appIdBasePath}/${STAGES_PATH}`);
      const stagesData = await stagesRes.json();
      const stagesDataClean = stagesData.filter((stage: Record<string, any>) =>  stage.status != "SKIPPED")
      const totalActiveTasks = stagesDataClean.map((stage: Record<string, any>) => stage.numActiveTasks).reduce((a: number, b: number) => a + b, 0);
      const totalPendingTasks = stagesDataClean.map((stage: Record<string, any>) => stage.numTasks - stage.numActiveTasks - stage.numFailedTasks - stage.numCompleteTasks).reduce((a: number, b: number) => a + b, 0);
      const totalInput = stagesDataClean.map((stage: Record<string, any>) => stage.inputBytes).reduce((a: number, b: number) => a + b, 0);
      const totalOutput = stagesDataClean.map((stage: Record<string, any>) => stage.outputBytes).reduce((a: number, b: number) => a + b, 0);
      const status = totalActiveTasks == 0 ? "idle" : "working";
      if(status == "idle"){
        POLL_TIME = IDLE_POLL_TIME
      } else {
        POLL_TIME = WORKING_POLL_TIME
      }
      const stats: StatusProps = {
        totalActiveTasks: totalActiveTasks,
        totalPendingTasks: totalPendingTasks,
        totalInput: humanFileSize(totalInput),
        totalOutput: humanFileSize(totalOutput),
        status: status
      }
      setStats(stats)
      // console.log("stats: " + JSON.stringify(stats));
    } catch (e) {
      console.log(e);
    }
  };


  React.useEffect(() => {
    fetchSparkData()
    const timer = setInterval(fetchSparkData, POLL_TIME)
    return () => clearInterval(timer)
  }, []);


  return (
    <Box sx={{ display: 'flex' }}>
      <AppBar appName={appName} />
      <Box
        component="main"
        sx={{
          backgroundColor: (theme) =>
            theme.palette.mode === 'light'
              ? theme.palette.grey[100]
              : theme.palette.grey[900],
          flexGrow: 1,
          height: '100vh',
          overflow: 'auto',
        }}
      >
        <Toolbar />
        <Grid container spacing={3} sx={{ mt: 2, mb: 2}} display="flex" justifyContent="center" alignItems="center">
          <InfoBox title="Status" text={stats?.status ?? ""} color="#7e57c2" icon={ApiIcon}></InfoBox>
          <InfoBox title="Input" text={stats?.totalInput ?? ""} color="#26a69a" icon={ArrowDownwardIcon}></InfoBox>
          <InfoBox title="Output" text={stats?.totalOutput ?? ""} color="#ffa726" icon={ArrowUpwardIcon}></InfoBox>
          <InfoBox title="Pending Tasks" text={stats?.totalPendingTasks?.toString() ?? ""} icon={QueueIcon}></InfoBox>
        </Grid>
        <Grid container spacing={3} sx={{ mt: 2, mb: 2}} display="flex" justifyContent="center" alignItems="center">
          <Grid item xs={16} md={8} lg={6}>
            <ConfigTable config={config} />
          </Grid>
        </Grid>
      </Box>
    </Box>
  );
}
