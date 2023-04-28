import * as React from 'react';
import AppBar from '@mui/material/AppBar';
import Box from '@mui/material/Box';
import Toolbar from '@mui/material/Toolbar';
import Typography from '@mui/material/Typography';
import Button from '@mui/material/Button';
import IconButton from '@mui/material/IconButton';
import FlareIcon from '@mui/icons-material/Flare';

export default function ButtonAppBar() {
  return (
    <AppBar position="absolute" color="primary" enableColorOnDark>
      <Toolbar>
        <FlareIcon style={{paddingRight: 10}} />
        <Typography variant="h6" component="div" sx={{ flexGrow: 1 }}>
          Upgraydd UI
        </Typography>
        <Button color="inherit">To Spark UI</Button>
      </Toolbar>
    </AppBar>
  );
}