import AutoAwesomeIcon from "@mui/icons-material/AutoAwesome";
import PaidOutlinedIcon from "@mui/icons-material/PaidOutlined";
import { Box, Button, Typography } from "@mui/material";
import React, { FC } from "react";
import { useAppSelector } from "../Hooks";
import {
  formatSavingsPercentage,
  openBookDemo,
  SAVINGS_BUTTON_SX,
  SAVINGS_COLOR,
  SAVINGS_TEXT_COLOR,
  shouldShowSavingsCta,
} from "../utils/BookDemo";

// Slim savings strip for the Status page, which is the first page users land on
const SavingsBanner: FC = (): JSX.Element | null => {
  const alerts = useAppSelector((state) => state.spark.alerts);
  const savings = alerts?.savingsOpportunity;
  if (savings === undefined || !shouldShowSavingsCta()) {
    return null;
  }
  const issuesCount = alerts?.alerts.length ?? 0;
  const issuesText =
    issuesCount === 0
      ? "mostly from idle cores"
      : `across ${issuesCount} issue${issuesCount === 1 ? "" : "s"} found`;

  return (
    <Box
      onClick={() => openBookDemo("status-banner", savings.totalPercentage)}
      sx={{
        mx: "auto",
        mb: 1,
        width: "fit-content",
        maxWidth: "95%",
        px: 2,
        py: 0.75,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        flexWrap: "wrap",
        gap: 1.5,
        cursor: "pointer",
        borderRadius: 2,
        border: `1px solid ${SAVINGS_COLOR}`,
        background:
          "linear-gradient(90deg, rgba(46,125,50,0.18), rgba(46,125,50,0.04))",
        "&:hover": { boxShadow: `0 2px 12px rgba(46,125,50,0.35)` },
      }}
    >
      <PaidOutlinedIcon sx={{ color: SAVINGS_TEXT_COLOR }} />
      <Typography variant="body1">
        You could save{" "}
        <b style={{ color: SAVINGS_TEXT_COLOR }}>
          ~{formatSavingsPercentage(savings.totalPercentage)}%
        </b>{" "}
        of this app's compute cost, {issuesText}
      </Typography>
      <Button
        size="small"
        variant="contained"
        startIcon={<AutoAwesomeIcon />}
        sx={SAVINGS_BUTTON_SX}
      >
        Fix it with AI · Book a demo
      </Button>
    </Box>
  );
};

export default SavingsBanner;
