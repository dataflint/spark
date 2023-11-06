import ErrorIcon from "@mui/icons-material/Error";
import WarningIcon from "@mui/icons-material/Warning";
import { Alert, AlertTitle } from "@mui/material";
import * as React from "react";
import { Alert as DataflintAlert } from "../../interfaces/AppStore";
import { TransperantTooltip } from "./AlertBadge";

type ToggableAlertProps = {
  alerts: DataflintAlert[];
};

export default function MultiAlertBadge({ alerts }: ToggableAlertProps) {
  const alert = alerts.length > 0 ? alerts[0] : undefined;
  return alert !== undefined ? (
    <TransperantTooltip
      placement={"bottom"}
      title={
        <React.Fragment>
          <Alert
            severity={alert.type}
            icon={alert.type === "warning" ? <WarningIcon /> : <ErrorIcon />}
          >
            <AlertTitle>{alert.title}</AlertTitle>
            {alert.message}
            {alerts.length > 1 ? (<React.Fragment><br /> + {alerts.length} additional alerts</React.Fragment>) : ""}
          </Alert>
        </React.Fragment>
      }
    >
      {alert.type === "warning" ? (
        <WarningIcon
          sx={{
            color: "#ff9100"
          }}
        ></WarningIcon>
      ) : (
        <ErrorIcon
          sx={{
            color: "#bf360c"
          }}
        ></ErrorIcon>
      )}
    </TransperantTooltip>
  ) : null;
}
