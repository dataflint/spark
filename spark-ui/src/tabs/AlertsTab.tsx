import { Alert, AlertTitle } from "@mui/material";
import { Stack } from "@mui/system";
import React from "react";
import { FC } from "react";
import { useAppSelector } from "../Hooks";

export const AlertsTab: FC<{}> = (): JSX.Element => {
  const alerts = useAppSelector((state) => state.spark.alerts);
  const errorsCount = alerts?.alerts.filter(alert => alert.type === "error").length;
  const warningsCount = alerts?.alerts.filter(alert => alert.type === "warning").length;

  return (
    <>
      {alerts?.alerts.length === 0 ?
        <div
          style={{
            height: "100%",
            display: "flex",
            justifyContent: "center",
            alignItems: "center"
          }}>
          <Alert severity="success">No alerts 😎</Alert>
        </div>
        :
        <div style={{ display: "flex", alignItems: "center", flexDirection: "column" }}>
          <div style={{ display: "flex", marginTop: "15px" }}>
            <Alert style={{ margin: "0 10px 0 10px" }} severity="error">{`Errors - ${errorsCount}`}</Alert>
            <Alert style={{ margin: "0 10px 0 10px" }} severity="warning">{`Alerts - ${warningsCount}`}</Alert>
          </div>
          <div style={{ overflow: "auto", height: "100%", padding: "15px", width: "100%" }}>
            <Stack sx={{ width: '100%', whiteSpace: "break-spaces" }} spacing={2}>
              {
                alerts?.alerts.map(alert => {
                  return <Alert key={alert.id} severity={alert.type}>
                    <AlertTitle>{alert.title}</AlertTitle>
                    {alert.message}
                    {"\n"}
                    {`Suggestions: ${alert.suggestion}`}
                  </Alert>
                })
              }
            </Stack>
          </div>
        </div>
      }
    </>
  );

}