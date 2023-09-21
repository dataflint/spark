import * as React from 'react';
import { SparkSQLStore, StatusStore } from '../interfaces/AppStore';
import SqlContainer from '../components/SqlContainer';
import SummaryBar from '../components/SummaryBar';

export default function SummaryTab({ status, sql }: { status: StatusStore, sql: SparkSQLStore | undefined }) {
  return (
    <div style={{display:"flex", height: "100%", flexDirection: "column"}}>
      <SummaryBar status={status}  />
      <SqlContainer currentSql={sql} />
    </div>
  );
}
