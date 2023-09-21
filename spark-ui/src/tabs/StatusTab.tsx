import * as React from 'react';
import { SparkExecutorsStatus, SparkSQLStore, StatusStore } from '../interfaces/AppStore';
import StatusBar from '../components/StatusBar';
import SqlContainer from '../components/SqlContainer';

export default function StatusTab({ status, sql, executorStatus }: { status: StatusStore | undefined, sql: SparkSQLStore | undefined, executorStatus: SparkExecutorsStatus | undefined }) {
  return (
    <div style={{display:"flex", height: "100%", flexDirection: "column"}}>
      <StatusBar currentStatus={status} executorStatus={executorStatus}  />
      <SqlContainer currentSql={sql} />
    </div>
  );
}