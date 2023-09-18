import React, { FC } from 'react';
import 'reactflow/dist/style.css';
import { Box, CircularProgress, Grid } from '@mui/material';
import { StatusStore } from '../interfaces/AppStore';
import Progress from './Progress';
import InfoBox from '../InfoBox';
import ArrowDownwardIcon from '@mui/icons-material/ArrowDownward';
import ApiIcon from '@mui/icons-material/Api';
import ArrowUpwardIcon from '@mui/icons-material/ArrowUpward';
import QueueIcon from '@mui/icons-material/Queue';


const StatusBar: FC<{currentStatus: StatusStore | undefined}> = (
    {currentStatus}): JSX.Element => {
        return currentStatus === undefined ?
        (
          <Progress />
        )
        : (<Grid container spacing={3} sx={{ mt: 2, mb: 2 }} display="flex" justifyContent="center" alignItems="center">
            <InfoBox title="Status" text={currentStatus.status} color="#7e57c2" icon={ApiIcon}></InfoBox>
            <InfoBox title="Input" text={currentStatus.totalInput} color="#26a69a" icon={ArrowDownwardIcon}></InfoBox>
            <InfoBox title="Output" text={currentStatus.totalOutput} color="#ffa726" icon={ArrowUpwardIcon}></InfoBox>
            <InfoBox title="Pending Tasks" text={currentStatus.totalPendingTasks.toString()} icon={QueueIcon}></InfoBox>
          </Grid>);
};

export default StatusBar;
