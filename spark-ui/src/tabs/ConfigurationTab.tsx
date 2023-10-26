import * as React from "react";
import Grid from "@mui/material/Grid";
import ConfigTable from "../components/ConfigTable";
import { MixpanelService } from "../services/MixpanelService";
import { useAppSelector } from "../Hooks";

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
