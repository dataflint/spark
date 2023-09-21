import React, { FC } from 'react';
import 'reactflow/dist/style.css';
import { Grid } from '@mui/material';
import { StatusStore } from '../interfaces/AppStore';
import Progress from './Progress';
import ArrowDownwardIcon from '@mui/icons-material/ArrowDownward';
import ArrowUpwardIcon from '@mui/icons-material/ArrowUpward';
import InfoBox from './InfoBox/InfoBox';
import { duration } from 'moment'
import { humanizeTimeDiff } from '../utils/FormatUtils';

const SummaryBar: FC<{status: StatusStore}> = (
    {status}): JSX.Element => {
      if(status.executors === undefined || status.stages === undefined) {
        return <Progress />;
      }

      const durationText = humanizeTimeDiff(duration(status.duration));

      return (<Grid container spacing={3} sx={{ mt: 2, mb: 2 }} display="flex" justifyContent="center" alignItems="center">
          <InfoBox title="Duration" text={durationText} color="#26a69a" icon={ArrowDownwardIcon}></InfoBox>
          <InfoBox title="Input" text={status.stages.totalInput} color="#26a69a" icon={ArrowDownwardIcon}></InfoBox>
          <InfoBox title="Output" text={status.stages.totalOutput} color="#ffa726" icon={ArrowUpwardIcon}></InfoBox>
        </Grid>);
};

export default SummaryBar;
