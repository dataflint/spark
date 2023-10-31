import { Alert, AlertTitle } from "@mui/material";
import { Stack } from "@mui/system";
import React from "react";
import { FC } from "react";
import { useAppSelector } from "../Hooks";

export const AlertsTab: FC<{}> = (): JSX.Element => {
    const alerts = useAppSelector((state) => state.spark.alerts);

    return (
        <div style={{ overflow: "auto", height: "100%", padding: "15px" }}>
            <Stack sx={{ width: '100%', whiteSpace: "break-spaces" }} spacing={2}>
                {
                    alerts?.alerts.map(alert => {
                        return <Alert severity={alert.type}>
                            <AlertTitle>{alert.title}</AlertTitle>
                            {alert.message}
                            {"\n"}
                            {`Suggestions: ${alert.suggestion}`}
                        </Alert>
                    })
                }
            </Stack>
        </div>
    );
}