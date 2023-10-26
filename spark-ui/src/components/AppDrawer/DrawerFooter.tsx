import Button from "@mui/material/Button";
import * as React from "react";
import { BASE_CURRENT_PAGE } from "../../utils/UrlConsts";
import { getBaseAppUrl } from "../../utils/UrlUtils";

export default function DrawerFooter({
  version,
}: {
  version?: string;
}) {
  const onSparkUiClick = (): void => {
    window.location.href = `${getBaseAppUrl(BASE_CURRENT_PAGE)}/jobs`;
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
