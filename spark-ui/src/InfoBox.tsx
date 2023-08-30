import * as React from 'react';
import Typography from '@mui/material/Typography';
import { Grid, Paper } from '@mui/material';
import PropTypes from "prop-types";
import styles from './InfoBox.module.css'; // Import css modules stylesheet as styles

type InfoBoxProps = {
  title: string,
  text: string,
  color?: string,
  icon: React.ElementType
}

export default function InfoBox({title, text, color, icon} : InfoBoxProps) {
  const Icon = icon;

  const [displayText, setDisplayText] = React.useState(text);
  const [blink, setBlink] = React.useState(false);

  React.useEffect(() => {
    setBlink(true);
    setDisplayText(text);
    const timer = setTimeout(() => {
      setBlink(false);
    }, 500);  // Animation duration
    return () => clearTimeout(timer);
  }, [text]);

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
        <Typography component="h2" variant="h6" color={color ?? "primary"} gutterBottom display="flex" justifyContent="center" alignItems="center">
            {title}
            <Icon sx={{ ml: 1}} />
        </Typography>
      <Typography component="p" variant="h4" display="flex" justifyContent="center" alignItems="center" className={blink ? styles.blink: ""}>
        {displayText}
      </Typography>
      {/* <Typography color="text.secondary" sx={{ flex: 1 }}>
        {secondaryText}
      </Typography> */}
    </React.Fragment>
  </Paper>
</Grid>

  );
}
