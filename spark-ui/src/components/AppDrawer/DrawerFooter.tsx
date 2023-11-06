import Button from "@mui/material/Button";
import * as React from "react";
import { BASE_CURRENT_PAGE, IS_HISTORY_SERVER_MODE } from "../../utils/UrlConsts";
import { getBaseAppUrl } from "../../utils/UrlUtils";

export default function DrawerFooter({ version }: { version?: string }) {
  const onSparkUiClick = (): void => {
    window.location.href = `${getBaseAppUrl(BASE_CURRENT_PAGE)}/jobs`;
  };

  const onHistoryServerClick = (): void => {
    window.location.href = `/history`;
  };

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        margin: "5px",
        alignItems: "center",
        fontSize: "12px",
      }}
    >
      <Button onClick={onSparkUiClick} color="inherit">
        To Spark UI
      </Button>
      {IS_HISTORY_SERVER_MODE ? <Button onClick={onHistoryServerClick} color="inherit">
        To History Server
      </Button> : null}
      {`Version ${version}`}
    </div>
  );
}
