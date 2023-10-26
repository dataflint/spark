import * as React from "react";
import Button from "@mui/material/Button";
import { getBaseAppUrl } from "../../utils/UrlUtils";

export default function DrawerFooter({
  appBasePath,
  version,
}: {
  appBasePath: string;
  version?: string;
}) {
  const onSparkUiClick = (): void => {
    window.location.href = `${getBaseAppUrl(appBasePath)}/jobs`;
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
      {`Version ${version}`}
    </div>
  );
}
