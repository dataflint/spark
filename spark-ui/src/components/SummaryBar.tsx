import React, { FC, useContext } from 'react';
import 'reactflow/dist/style.css';
import { Grid } from '@mui/material';
import { StatusStore } from '../interfaces/AppStore';
import Progress from './Progress';
import ArrowDownwardIcon from '@mui/icons-material/ArrowDownward';
import ArrowUpwardIcon from '@mui/icons-material/ArrowUpward';
import InfoBox from './InfoBox/InfoBox';
import { duration } from 'moment'
import { humanizeTimeDiff } from '../utils/FormatUtils';
import DirectionsRunIcon from '@mui/icons-material/DirectionsRun';
import AccessTimeIcon from '@mui/icons-material/AccessTime';
import MemoryIcon from '@mui/icons-material/Memory';
import { useAppSelector } from '../Hooks';

const SummaryBar: FC = (): JSX.Element => {
  const status = useAppSelector(state => state.spark.status);

  if (status?.executors === undefined || status.stages === undefined) {
    return <Progress />;
  }

  const durationText = humanizeTimeDiff(duration(status.duration));

  return (<Grid container spacing={3} sx={{ mt: 2, mb: 2 }} display="flex" justifyContent="center" alignItems="center">
    <InfoBox title="Duration" text={durationText} color="#a31545" icon={AccessTimeIcon}></InfoBox>
    <InfoBox title="Core Hour" text={status.executors.totalCoreHour.toFixed(2)} color="#795548" icon={MemoryIcon}></InfoBox>
    <InfoBox title="Input" text={status.stages.totalInput} color="#26a69a" icon={ArrowDownwardIcon}></InfoBox>
    <InfoBox title="Output" text={status.stages.totalOutput} color="#ffa726" icon={ArrowUpwardIcon}></InfoBox>
    <InfoBox title="Activity Rate" text={status.executors.activityRate.toFixed(2) + "%"} color="#618833" icon={DirectionsRunIcon}></InfoBox>
  </Grid>);
};

export default SummaryBar;
