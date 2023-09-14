import * as React from 'react';
import Grid from '@mui/material/Grid';
import InfoBox from './InfoBox';
import ApiIcon from '@mui/icons-material/Api';
import ArrowUpwardIcon from '@mui/icons-material/ArrowUpward';
import ArrowDownwardIcon from '@mui/icons-material/ArrowDownward';
import QueueIcon from '@mui/icons-material/Queue';
import { AppStore } from './interfaces/AppStore';
import SqlFlow from './components/SqlFlow/SqlFlow';
import { Stack } from '@mui/material';

export default function StatusTab({ store }: { store: AppStore }) {
  return (
    <React.Fragment>
      <Grid container spacing={3} sx={{ mt: 2, mb: 2 }} display="flex" justifyContent="center" alignItems="center">
        <InfoBox title="Status" text={store.status.status} color="#7e57c2" icon={ApiIcon}></InfoBox>
        <InfoBox title="Input" text={store.status.totalInput} color="#26a69a" icon={ArrowDownwardIcon}></InfoBox>
        <InfoBox title="Output" text={store.status.totalOutput} color="#ffa726" icon={ArrowUpwardIcon}></InfoBox>
        <InfoBox title="Pending Tasks" text={store.status.totalPendingTasks.toString()} icon={QueueIcon}></InfoBox>
      </Grid>
      <div style={{ height: '50%'}}>
        <SqlFlow sparkSQLs={store.sql} />
      </div>
    </React.Fragment>
  );
}