import * as React from 'react';
import AppBar from '@mui/material/AppBar';
import Toolbar from '@mui/material/Toolbar';
import Typography from '@mui/material/Typography';
import Button from '@mui/material/Button';
import FlareIcon from '@mui/icons-material/Flare';

export default function DevtoolAppBar({appName} : {appName: string}) {
  return (
      <Toolbar>
        <FlareIcon style={{paddingRight: 10}} />
        <Typography variant="h6" component="div" sx={{ flexGrow: 1 }}>
          Spark Dev Tool UI - {appName}
        </Typography>
        <Button href="/jobs" color="inherit" >To Spark UI</Button>
      </Toolbar>
  );
}