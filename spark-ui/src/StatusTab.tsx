import * as React from 'react';
import { SparkSQLStore, StatusStore } from './interfaces/AppStore';
import StatusBar from './components/StatusBar';
import SqlContainer from './components/SqlContainer';

export default function StatusTab({ status, sql }: { status: StatusStore | undefined, sql: SparkSQLStore | undefined }) {
  return (
    <div style={{display:"flex", height: "100%", flexDirection: "column"}}>
      <StatusBar currentStatus={status} />
      <SqlContainer currentSql={sql} />
    </div>
  );
}