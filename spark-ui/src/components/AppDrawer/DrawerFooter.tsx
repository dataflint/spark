import * as React from 'react';
import Button from '@mui/material/Button';

export default function DrawerFooter({ version }: { version?: string }) {
  return (
    <div style={{ display: "flex", flexDirection: "column" , margin: "5px", alignItems: "center", fontSize:"12px"}}>
      <Button href="/jobs" color="inherit" >To Spark UI</Button>
      {`Version ${version}`}
    </div>
  );
}