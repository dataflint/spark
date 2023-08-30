import * as React from 'react';
import AppBar from '@mui/material/AppBar';
import Toolbar from '@mui/material/Toolbar';
import Typography from '@mui/material/Typography';
import Button from '@mui/material/Button';
import FlareIcon from '@mui/icons-material/Flare';

export default function ButtonAppBar({appName} : {appName: string}) {
  return (
    <AppBar position="absolute" color="primary" enableColorOnDark>
      <Toolbar>
        <FlareIcon style={{paddingRight: 10}} />
        <Typography variant="h6" component="div" sx={{ flexGrow: 1 }}>
          Spark Dev tool UI - {appName}
        </Typography>
        <Button href="/jobs" color="inherit" >To Spark UI</Button>
      </Toolbar>
    </AppBar>
  );
}