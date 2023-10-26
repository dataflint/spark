import { Grid, Paper, Tooltip } from "@mui/material";
import Typography from "@mui/material/Typography";
import * as React from "react";
import styles from "./InfoBox.module.css"; // Import css modules stylesheet as styles

type InfoBoxProps = {
  title: string;
  text: string;
  color?: string;
  icon: React.ElementType;
  tooltipContent?: JSX.Element
};

const ConditionalWrapper = ({ condition, wrapper, children }: { condition: boolean, wrapper: (children: JSX.Element) => JSX.Element, children: JSX.Element }) =>
  condition ? wrapper(children) : children;

export default function InfoBox({ title, text, color, icon, tooltipContent }: InfoBoxProps) {
  const Icon = icon;
  const [blink, setBlink] = React.useState(false);

  React.useEffect(() => {
    setBlink(true);
    const timer = setTimeout(() => {
      setBlink(false);
    }, 500); // Animation duration
    return () => clearTimeout(timer);
  }, [text]);

  return (
    <Grid item lg={2}>
      <ConditionalWrapper condition={tooltipContent !== undefined} wrapper={(childern) => (<Tooltip title={tooltipContent}>{childern}</Tooltip>)}>
        <Paper
          sx={{
            p: 2,
            display: "flex",
            flexDirection: "column",
            height: 110,
          }}
        >
          <React.Fragment>
            <Typography
              component="h2"
              variant="h6"
              color={color ?? "primary"}
              gutterBottom
              display="flex"
              justifyContent="center"
              alignItems="center"
            >
              {title}
              <Icon sx={{ ml: 1 }} />
            </Typography>
            <Typography
              component="p"
              variant="h4"
              display="flex"
              justifyContent="center"
              alignItems="center"
              className={blink ? styles.blink : ""}
            >
              {text}
            </Typography>
            {/* <Typography color="text.secondary" sx={{ flex: 1 }}>
        {secondaryText}
      </Typography> */}
          </React.Fragment>
        </Paper>
      </ConditionalWrapper>
    </Grid>
  );
}
