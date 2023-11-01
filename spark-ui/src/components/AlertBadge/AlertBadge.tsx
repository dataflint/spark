import ErrorIcon from "@mui/icons-material/Error";
import WarningIcon from "@mui/icons-material/Warning";
import { Alert, AlertTitle, styled } from "@mui/material";
import Tooltip, { tooltipClasses, TooltipProps } from "@mui/material/Tooltip";
import * as React from "react";
import { Alert as DataflintAlert } from "../../interfaces/AppStore";

type InfoBoxProps = {
  alert?: DataflintAlert;
  margin?: string;
  placement?:
    | "top"
    | "right"
    | "bottom"
    | "left"
    | "bottom-end"
    | "bottom-start"
    | "left-end"
    | "left-start"
    | "right-end"
    | "right-start"
    | "top-end"
    | "top-start";
};

const TransperantTooltip = styled(({ className, ...props }: TooltipProps) => (
  <Tooltip {...props} classes={{ popper: className }} />
))(({ theme }) => ({
  [`& .${tooltipClasses.tooltip}`]: {
    backgroundColor: "transparent",
  },
}));

export default function AlertBadge({ alert, margin, placement }: InfoBoxProps) {
  return alert !== undefined ? (
    <TransperantTooltip
      placement={placement ?? "top"}
      title={
        <React.Fragment>
          <Alert
            severity={alert.type}
            icon={alert.type === "warning" ? <WarningIcon /> : <ErrorIcon />}
          >
            <AlertTitle>{alert.title}</AlertTitle>
            {alert.message}
          </Alert>
        </React.Fragment>
      }
    >
      {alert.type === "warning" ? (
        <WarningIcon
          sx={{
            color: "#ff9100",
            position: "absolute",
            top: "0%",
            right: "0%",
            transform: "translate(50%, -50%)",
            margin: margin ?? "0px",
          }}
        ></WarningIcon>
      ) : (
        <ErrorIcon
          sx={{
            color: "#bf360c",
            position: "absolute",
            top: "0%",
            right: "0%",
            transform: "translate(50%, -50%)",
            margin: margin ?? "0px",
          }}
        ></ErrorIcon>
      )}
    </TransperantTooltip>
  ) : null;
}
