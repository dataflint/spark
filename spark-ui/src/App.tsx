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
import SparkAPI from './services/SparkApi';
import { AppStore } from './interfaces/AppStore';
import { CircularProgress } from '@mui/material';

let BASE_PATH = ""
if (process.env.NODE_ENV === 'development') {
  BASE_PATH = process.env.REACT_APP_BASE_PATH ?? ""
}

export default function App() {
  const [store, setStore] = React.useState<AppStore>();

  React.useEffect(() => {
    const sparkAPI = new SparkAPI(BASE_PATH, setStore, () => store)
    const cleanerFunc = sparkAPI.start();
    return cleanerFunc;
  }, []);

  return (
    store === undefined ?
      (
        <Box
          display="flex"
          justifyContent="center"
          alignItems="center"
          minHeight="100vh"
        >
          <CircularProgress />
        </Box>
      )
      :
      (<Box sx={{ display: 'flex' }}>
        <AppBar appName={store.appName} />
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
          <Grid container spacing={3} sx={{ mt: 2, mb: 2  }} display="flex" justifyContent="center" alignItems="center">
            <InfoBox title="Status" text={store.status.status} color="#7e57c2" icon={ApiIcon}></InfoBox>
            <InfoBox title="Input" text={store.status.totalInput} color="#26a69a" icon={ArrowDownwardIcon}></InfoBox>
            <InfoBox title="Output" text={store.status.totalOutput} color="#ffa726" icon={ArrowUpwardIcon}></InfoBox>
            <InfoBox title="Pending Tasks" text={store.status.totalPendingTasks.toString()} icon={QueueIcon}></InfoBox>
          </Grid>
          <div style={{ height: '50%' }}>
          <SqlFlow initNodes={store.sql[0].nodes} initEdges={store.sql[0].edges}/>
        </div>
        <Grid container spacing={3} sx={{ mt: 2, mb: 2  }} display="flex" justifyContent="center" alignItems="center">
            <Grid item xs={16} md={8} lg={6}>
              <ConfigTable config={store.config} />
            </Grid>
          </Grid>
        </Box>
      </Box>));
}
