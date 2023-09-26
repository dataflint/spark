import * as React from 'react';
import { SparkExecutorsStatus, SparkSQLStore, StagesSummeryStore, StatusStore } from '../interfaces/AppStore';
import StatusBar from '../components/StatusBar';
import SqlContainer from '../components/SqlContainer';

export default function StatusTab({ status, sql }: { status: StatusStore, sql: SparkSQLStore | undefined }) {
  const currentSql = sql === undefined ? undefined : sql.sqls[sql.sqls.length - 1]
  return (
    <div style={{display:"flex", height: "100%", flexDirection: "column"}}>
      <StatusBar stagesStatus={status.stages} executorStatus={status.executors} sql={currentSql}  />
      <SqlContainer sql={currentSql} />
    </div>
  );
}