import * as React from 'react';
import SummaryBar from '../components/SummaryBar';
import SqlTable from '../components/SqlTable/SqlTable';
import SqlFlow from '../components/SqlFlow/SqlFlow';
import { Button, Fade } from '@mui/material';
import { AppStateContext } from '../Context';
import mixpanel from 'mixpanel-browser';
import { MixpanelEvents } from '../interfaces/Mixpanel';
import { MixpanelService } from '../services/MixpanelService';


export default function SummaryTab() {
  const { sql } = React.useContext(AppStateContext);
  const [selectedSqlId, setSelectedSqlId] = React.useState<string | undefined>(undefined);
  const selectedSql = selectedSqlId === undefined ? undefined : sql?.sqls.find(sql => sql.id === selectedSqlId);

  React.useEffect(() => {
    MixpanelService.TrackPageView();
  }, [])

  React.useEffect(() => {
    function handleEscapeKey(event: KeyboardEvent) {
      if (event.code === 'Escape') {
        setSelectedSqlId(undefined);
      }
    }

    document.addEventListener('keydown', handleEscapeKey)
    return () => document.removeEventListener('keydown', handleEscapeKey)
  }, []);

  const onSelectingSql = (id: string) => {
    setSelectedSqlId(id);
    const currentSql = sql?.sqls.find(sql => sql.id === id);

    MixpanelService.Track(MixpanelEvents.SqlSummarySelected, {
      sqlId: currentSql?.id,
      sqluniqueId: currentSql?.uniqueId,
      sqlStatus: currentSql?.status,
      sqlSubmissionTime: currentSql?.submissionTime,
      sqlDuration: currentSql?.duration
    });
  }

  return (
    <div style={{ overflow: "hidden", height: "100%" }}>
      {selectedSql === undefined ?
        <Fade in={selectedSqlId === undefined} style={{}}>
          <div style={{ display: "flex", height: "100%", flexDirection: "column" }}>
            <SummaryBar />
            <SqlTable sqlStore={sql} selectedSqlId={selectedSqlId} setSelectedSqlId={onSelectingSql} />
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
