import * as React from 'react';
import Typography from '@mui/material/Typography';
import { Grid, Paper } from '@mui/material';

type InfoBoxProps = {
  title: string,
  text: string,
}

export default function InfoBox({title, text} : InfoBoxProps) {
  return (
    <Grid item lg={2}>
    <Paper
      sx={{
        p: 2,
        display: 'flex',
        flexDirection: 'column',
        height: 120,
      }}
    >
      <React.Fragment>
        <Typography component="h2" variant="h6" color="primary" gutterBottom display="flex" justifyContent="center" alignItems="center">
            {title}
        </Typography>
      <Typography component="p" variant="h4" display="flex" justifyContent="center" alignItems="center">
        {text}
      </Typography>
      {/* <Typography color="text.secondary" sx={{ flex: 1 }}>
        {secondaryText}
      </Typography> */}
    </React.Fragment>
  </Paper>
</Grid>

  );
}
