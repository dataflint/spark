import Grid from "@mui/material/Grid";
import * as React from "react";
import ConfigTable from "../components/ConfigTable";
import { useAppSelector } from "../Hooks";
import { MixpanelService } from "../services/MixpanelService";

export default function ConfigurationTab() {
  const config = useAppSelector((state) => state.spark.config);

  React.useEffect(() => {
    MixpanelService.TrackPageView();
  }, []);

  return (
    <Grid
      container
      spacing={3}
      sx={{ mt: 2, mb: 2 }}
      display="flex"
      justifyContent="center"
      alignItems="center"
    >
      <Grid item xs={16} md={8} lg={6}>
        {!!config && <ConfigTable config={config} />}
      </Grid>
    </Grid>
  );
}
