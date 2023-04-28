import * as React from 'react';
import Box from '@mui/material/Box';
import AppBar from './AppBar';
import Toolbar from '@mui/material/Toolbar';
import Container from '@mui/material/Container';
import Grid from '@mui/material/Grid';
import Paper from '@mui/material/Paper';
import InfoBox from './InfoBox';
import ConfigTable from './ConfigTable';

const POLL_TIME = 5000
const BASE_PATH = "http://desktop-qeuprph:4040"
const API_PATH = "api/v1"
const APPLICATION_PATH = "applications"
const APPLICATION_ENCIRONMENT_PATH = "environment"

// http://desktop-qeuprph:4040/api/v1/applications/local-1682685376172/environment
// process.env.SPARK_UI_URI

export default function App() {

  const [appId, setAppId] = React.useState("");

  const [generalConfig, setGeneralConfig] = React.useState({});

  const [config, setConfig] = React.useState({});

  const fetchSparkData = async () => {
    try {
      var currentAppId = appId
      if (!appId) {
        const app_res = await fetch(`${BASE_PATH}/${API_PATH}/${APPLICATION_PATH}`);
        const app_data = await app_res.json();
        currentAppId = app_data[0].id;
        const generalConfig = app_data[0].attempts[0];
        setAppId(currentAppId);
        setGeneralConfig({
          "Start Time": generalConfig.startTime,
          "Spark Version": generalConfig.appSparkVersion
        });
      }

      if (!config) {
        const app_env_res = await fetch(`${BASE_PATH}/${API_PATH}/${APPLICATION_PATH}/${currentAppId}/${APPLICATION_ENCIRONMENT_PATH}`);
        const app_env_data = await app_env_res.json();
  
        const sparkPropertiesObj = Object.fromEntries(app_env_data.sparkProperties);
  
        console.log("spark properties: " + JSON.stringify(sparkPropertiesObj))
  
        setConfig({
            "spark.app.name": sparkPropertiesObj["spark.app.name"],
            "spark.app.id	": sparkPropertiesObj["spark.app.id"]
          });
      }
    } catch (e) {
      console.log(e);
    }
  };
  

  React.useEffect(() => {
    fetchSparkData()
    const ptr = setInterval(fetchSparkData, POLL_TIME)
    return () => clearInterval(ptr)
  }, []);


  return (
    <Box sx={{ display: 'flex' }}>
      <AppBar />
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
          <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
            <Grid container spacing={3}>
            <Grid item xs={16} md={8} lg={6}>
              <ConfigTable config={generalConfig} />
            </Grid>
            
              {/* Chart */}
              {/* <Grid item xs={12} md={8} lg={9}>
                <Paper
                  sx={{
                    p: 2,
                    display: 'flex',
                    flexDirection: 'column',
                    height: 240,
                  }}
                >
                  <Chart />
                </Paper>
              </Grid> */}
              {/* Recent Deposits RELEVANT */}
              {/* <Grid item xs={12} md={4} lg={3}>
                <Paper
                  sx={{
                    p: 2,
                    display: 'flex',
                    flexDirection: 'column',
                    height: 240,
                  }}
                >
                  <InfoBox />
                </Paper>
              </Grid> */}
              {/* Recent Orders */}
              {/* <Grid item xs={12}>
                <Paper sx={{ p: 2, display: 'flex', flexDirection: 'column' }}>
                  <Orders />
                </Paper>
              </Grid>
             */}
             </Grid>
          </Container>
        </Box>
      </Box>
  );
}
