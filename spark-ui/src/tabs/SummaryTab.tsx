import ArrowBackIcon from "@mui/icons-material/ArrowBack";
import BuildIcon from "@mui/icons-material/Build";
import { Box, Fade, IconButton, Tooltip, Typography } from "@mui/material";
import * as React from "react";
import { useSearchParams } from "react-router-dom";
import SqlFlow from "../components/SqlFlow/SqlFlow";
import SqlTable from "../components/SqlTable/SqlTable";
import SummaryBar from "../components/SummaryBar";
import { useAppSelector } from "../Hooks";
import { MixpanelEvents } from "../interfaces/Mixpanel";
import { MixpanelService } from "../services/MixpanelService";
import { BASE_CURRENT_PAGE } from "../utils/UrlConsts";
import { getBaseAppUrl } from "../utils/UrlUtils";

export default function SummaryTab() {
  const sql = useAppSelector((state) => state.spark.sql);
  const [searchParams, setSearchParams] = useSearchParams();

  const selectedSqlId = searchParams.get("sqlid") || undefined;
  const selectedSql =
    selectedSqlId === undefined
      ? undefined
      : sql?.sqls.find((sql) => {
        const sqlIdNum = parseInt(sql.id, 10);
        const selectedIdNum = parseInt(selectedSqlId, 10);
        return sqlIdNum === selectedIdNum;
      });

  React.useEffect(() => {
    MixpanelService.TrackPageView();
  }, []);

  React.useEffect(() => {
    function handleEscapeKey(event: KeyboardEvent) {
      if (event.code === "Escape") {
        const newParams = new URLSearchParams(searchParams);
        newParams.delete("sqlid");
        setSearchParams(newParams);
      }
    }

    document.addEventListener("keydown", handleEscapeKey);
    return () => document.removeEventListener("keydown", handleEscapeKey);
  }, [searchParams, setSearchParams]);

  const onSparkUiSQLClick = (): void => {
    window.open(
      `${getBaseAppUrl(BASE_CURRENT_PAGE)}/SQL/execution/?id=${selectedSqlId}`,
      "_blank",
    );
  };

  const onSelectingSql = (id: string) => {
    const newParams = new URLSearchParams(searchParams);
    newParams.set("sqlid", id);
    setSearchParams(newParams);

    const currentSql = sql?.sqls.find((sql) => sql.id === id);

    MixpanelService.Track(MixpanelEvents.SqlSummarySelected, {
      sqlId: currentSql?.id,
      sqluniqueId: currentSql?.uniqueId,
      sqlStatus: currentSql?.status,
      sqlSubmissionTime: currentSql?.submissionTime,
      sqlDuration: currentSql?.duration,
    });
  };

  const onBackClick = () => {
    const newParams = new URLSearchParams(searchParams);
    newParams.delete("sqlid");
    setSearchParams(newParams);
  };

  const tableDisplay = selectedSqlId === undefined ? "flex" : "none";

  return (
    <div style={{ overflow: "hidden", height: "100%" }}>
      <Fade timeout={300} in={selectedSqlId === undefined} style={{}}>
        <div
          style={{
            display: tableDisplay,
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
          <Box display={"flex"}>
            <Tooltip title="Back">
              <IconButton
                color="primary"
                onClick={onBackClick}
              >
                <ArrowBackIcon style={{ width: "40px", height: "40px" }} />
              </IconButton>
            </Tooltip>
            <Tooltip title="Spark UI SQL View">
              <IconButton color="secondary" onClick={() => onSparkUiSQLClick()}>
                <BuildIcon style={{ width: "30px", height: "30px" }} />
              </IconButton>
            </Tooltip>
            <Box
              marginLeft="10px"
              display="flex"
              alignItems="center"
              alignContent="center"
            >
              <Typography variant="h5">
                query {selectedSql?.id}: {selectedSql?.description}
              </Typography>
            </Box>
          </Box>
          {selectedSql !== undefined ? (
            <SqlFlow sparkSQL={selectedSql} />
          ) : null}
        </div>
      </Fade>
    </div>
  );
}
