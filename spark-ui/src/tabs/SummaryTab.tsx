import * as React from 'react';
import { EnrichedSparkSQL, SparkSQLStore, StatusStore } from '../interfaces/AppStore';
import SummaryBar from '../components/SummaryBar';
import SqlTable from '../components/SqlTable/SqlTable';
import Stack from '@mui/material/Stack';
import SqlContainer from '../components/SqlContainer';
import SqlFlow from '../components/SqlFlow/SqlFlow';
import { Button, Fade, Grow } from '@mui/material';


export default function SummaryTab({ status, sql }: { status: StatusStore, sql: SparkSQLStore | undefined }) {
  const [selectedSqlId, setSelectedSqlId] = React.useState<string | undefined>(undefined);
  const selectedSql = selectedSqlId === undefined ? undefined : sql?.sqls.find(sql => sql.id === selectedSqlId);

  return (
    <div style={{ height: '100vh', width: '100vw'}}>
      <Fade in={selectedSqlId === undefined} style={{height: "100vh", width: '100vw', top: 0, left: 0, position: "absolute"}}>
        <div>
          <div style={{display: "flex", height: "100vh", flexDirection: "column"}}>
            <SummaryBar status={status} />
            <SqlTable sqlStore={sql} selectedSqlId={selectedSqlId} setSelectedSqlId={setSelectedSqlId} />
          </div>
        </div>
        </Fade>
        <Fade in={selectedSqlId !== undefined} style={{height: "100vh", width: '100vw', top: 0, left: 0, position: "absolute"}}>
          { selectedSql === undefined ? <div></div> : 
          <div style={{ height: '100vh', width: '100vw'}}>
              <Button onClick={() => setSelectedSqlId(undefined)} style={{top: 500, left: 500, position: "absolute", zIndex: 10}} variant="outlined">Outlined</Button>
              <SqlFlow sparkSQL={selectedSql} />
            </div>  }
        </Fade>
      </div>
  );
}
