import * as React from 'react';
import SummaryBar from '../components/SummaryBar';
import SqlTable from '../components/SqlTable/SqlTable';
import SqlFlow from '../components/SqlFlow/SqlFlow';
import { Button, Fade } from '@mui/material';
import { AppStateContext } from '../Context';


export default function SummaryTab() {
  const { sql } = React.useContext(AppStateContext);
  const [selectedSqlId, setSelectedSqlId] = React.useState<string | undefined>(undefined);
  const selectedSql = selectedSqlId === undefined ? undefined : sql?.sqls.find(sql => sql.id === selectedSqlId);

  React.useEffect(() => {
    function handleEscapeKey(event: KeyboardEvent) {
      if (event.code === 'Escape') {
        setSelectedSqlId(undefined);
      }
    }

    document.addEventListener('keydown', handleEscapeKey)
    return () => document.removeEventListener('keydown', handleEscapeKey)
  }, [])

  return (
    <div style={{ overflow: "hidden", height: "100%" }}>
      {selectedSql === undefined ?
        <Fade in={selectedSqlId === undefined} style={{}}>
          <div style={{ display: "flex", height: "100%", flexDirection: "column" }}>
            <SummaryBar />
            <SqlTable sqlStore={sql} selectedSqlId={selectedSqlId} setSelectedSqlId={setSelectedSqlId} />
          </div>
        </Fade>
        :
        <Fade in={selectedSqlId !== undefined} style={{}}>
          <div style={{ display: 'flex', height: '100%', width: '100%', flexDirection: "column" }}>
            <Button onClick={() => setSelectedSqlId(undefined)} style={{ width: "100px", margin: "10px" }} variant="outlined">Back</Button>
            <SqlFlow sparkSQL={selectedSql} />
          </div>
        </Fade>
      }
    </div>
  );
}
