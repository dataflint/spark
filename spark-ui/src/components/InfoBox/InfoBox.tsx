import { Box, Grid, Paper, styled } from "@mui/material";
import Tooltip, { tooltipClasses, TooltipProps } from "@mui/material/Tooltip";
import Typography from "@mui/material/Typography";
import * as React from "react";
import { Alert as DataflintAlert } from "../../interfaces/AppStore";
import AlertBadge from "../AlertBadge/AlertBadge";
import styles from "./InfoBox.module.css"; // Import css modules stylesheet as styles

type InfoBoxProps = {
  title: string;
  text: string;
  color?: string;
  icon: React.ElementType;
  tooltipContent?: JSX.Element;
  alert?: DataflintAlert;
};

const TransperantTooltip = styled(({ className, ...props }: TooltipProps) => (
  <Tooltip {...props} classes={{ popper: className }} />
))(({ theme }) => ({
  [`& .${tooltipClasses.tooltip}`]: {
    backgroundColor: "transparent",
  },
}));

export const ConditionalWrapper = ({
  condition,
  wrapper,
  children,
}: {
  condition: boolean;
  wrapper: (children: JSX.Element) => JSX.Element;
  children: JSX.Element;
}) => (condition ? wrapper(children) : children);

export default function InfoBox({
  title,
  text,
  color,
  icon,
  tooltipContent,
  alert,
}: InfoBoxProps) {
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
      <Box position="relative">
        <ConditionalWrapper
          condition={tooltipContent !== undefined}
          wrapper={(childern) => (
            <Tooltip title={tooltipContent}>{childern}</Tooltip>
          )}
        >
          <Paper
            sx={{
              p: 2,
              display: "flex",
              flexDirection: "column",
              height: 110,
              position: "relative",
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
            </React.Fragment>
          </Paper>
        </ConditionalWrapper>
        <AlertBadge alert={alert} />
      </Box>
    </Grid>
  );
}
