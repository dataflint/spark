import { Button, Fade } from "@mui/material";
import * as React from "react";
import SqlFlow from "../components/SqlFlow/SqlFlow";
import SqlTable from "../components/SqlTable/SqlTable";
import SummaryBar from "../components/SummaryBar";
import { useAppSelector } from "../Hooks";
import { MixpanelEvents } from "../interfaces/Mixpanel";
import { MixpanelService } from "../services/MixpanelService";

export default function SummaryTab() {
  const sql = useAppSelector((state) => state.spark.sql);
  const [selectedSqlId, setSelectedSqlId] = React.useState<string | undefined>(
    undefined,
  );
  const selectedSql =
    selectedSqlId === undefined
      ? undefined
      : sql?.sqls.find((sql) => sql.id === selectedSqlId);

  React.useEffect(() => {
    MixpanelService.TrackPageView();
  }, []);

  React.useEffect(() => {
    function handleEscapeKey(event: KeyboardEvent) {
      if (event.code === "Escape") {
        setSelectedSqlId(undefined);
      }
    }

    document.addEventListener("keydown", handleEscapeKey);
    return () => document.removeEventListener("keydown", handleEscapeKey);
  }, []);

  const onSelectingSql = (id: string) => {
    setSelectedSqlId(id);
    const currentSql = sql?.sqls.find((sql) => sql.id === id);

    MixpanelService.Track(MixpanelEvents.SqlSummarySelected, {
      sqlId: currentSql?.id,
      sqluniqueId: currentSql?.uniqueId,
      sqlStatus: currentSql?.status,
      sqlSubmissionTime: currentSql?.submissionTime,
      sqlDuration: currentSql?.duration,
    });
  };

  const talbeDisplay = !selectedSqlId ? "flex" : "none";

  return (
    <div style={{ overflow: "hidden", height: "100%" }}>
      <Fade timeout={300} in={selectedSqlId === undefined} style={{}}>
        <div
          style={{
            display: talbeDisplay,
            height: "100%",
            flexDirection: "column",
          }}
        >
          <SummaryBar />
          <SqlTable
            sqlStore={sql}
            selectedSqlId={selectedSqlId}
            setSelectedSqlId={onSelectingSql}
          />
        </div>
      </Fade>
      <Fade timeout={300} in={selectedSqlId !== undefined} style={{}}>
        <div
          style={{
            display: "flex",
            height: "100%",
            width: "100%",
            flexDirection: "column",
          }}
        >
          <Button
            onClick={() => setSelectedSqlId(undefined)}
            style={{ width: "100px", margin: "10px" }}
            variant="outlined"
          >
            Back
          </Button>
          {!!selectedSql && <SqlFlow sparkSQL={selectedSql} />}
        </div>
      </Fade>
    </div>
  );
}
