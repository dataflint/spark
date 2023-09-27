import React, { FC, useContext } from 'react';
import 'reactflow/dist/style.css';
import { Grid } from '@mui/material';
import { EnrichedSparkSQL, SparkExecutorsStatus, StagesSummeryStore } from '../interfaces/AppStore';
import Progress from './Progress';
import ArrowDownwardIcon from '@mui/icons-material/ArrowDownward';
import ApiIcon from '@mui/icons-material/Api';
import ArrowUpwardIcon from '@mui/icons-material/ArrowUpward';
import QueueIcon from '@mui/icons-material/Queue';
import WorkIcon from '@mui/icons-material/Work';
import InfoBox from './InfoBox/InfoBox';
import { humanFileSize } from '../utils/FormatUtils';
import { AppStateContext } from '../Context';




const StatusBar: FC = (): JSX.Element => {
  const { status, sql } = useContext(AppStateContext);

  const currentSql = sql === undefined ? undefined : sql.sqls[sql.sqls.length - 1]
  const stagesStatus = status?.stages;
  const executorStatus = status?.executors;
  if (stagesStatus === undefined || executorStatus === undefined) {
    return <Progress />;
  }

  const numOfExecutorsText = executorStatus.numOfExecutors === 0 ? "1 (driver)" : executorStatus.numOfExecutors.toString();

  return (<Grid container spacing={3} sx={{ mt: 2, mb: 2 }} display="flex" justifyContent="center" alignItems="center">
    <InfoBox title="Status" text={stagesStatus.status} color="#7e57c2" icon={ApiIcon}></InfoBox>
    {currentSql === undefined || currentSql.stageMetrics === undefined ? null : <InfoBox title="Query Input" text={humanFileSize(sql.stageMetrics.inputBytes)} color="#26a69a" icon={ArrowDownwardIcon}></InfoBox>}
    {currentSql === undefined || currentSql.stageMetrics === undefined ? null : <InfoBox title="Query Output" text={humanFileSize(sql.stageMetrics.outputBytes)} color="#ffa726" icon={ArrowUpwardIcon}></InfoBox>}
    <InfoBox title="Executors" text={numOfExecutorsText} color="#52b202" icon={WorkIcon}></InfoBox>
    <InfoBox title="Pending Tasks" text={stagesStatus.totalPendingTasks.toString()} icon={QueueIcon}></InfoBox>
  </Grid>);
};

export default StatusBar;
