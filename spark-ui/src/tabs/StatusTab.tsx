import * as React from 'react';
import StatusBar from '../components/StatusBar';
import SqlContainer from '../components/SqlContainer';
import { AppStateContext } from '../Context';
import { MixpanelService } from '../services/MixpanelService';

export default function StatusTab() {
  const { sql } = React.useContext(AppStateContext);

  React.useEffect(() => {
    MixpanelService.TrackPageView();
  }, [])

  return (
    <div style={{ display: "flex", height: "100%", flexDirection: "column" }}>
      <StatusBar />
      {!!sql && sql.sqls.length > 0 &&
        <div style={{ textAlign: "center", display: "block", fontSize: "1.5em", fontWeight: "normal", margin: "2px 0 5px 0" }}>
          {sql.sqls[sql.sqls.length - 1].description}
        </div>}
      <SqlContainer />
    </div >
  );
}