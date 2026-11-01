import CodeIcon from "@mui/icons-material/Code";
import PaidOutlinedIcon from "@mui/icons-material/PaidOutlined";
import { Box, Button, Typography } from "@mui/material";
import React, { FC } from "react";
import { useAppSelector } from "../Hooks";
import {
  formatSavingsPercentage,
  openBookDemo,
  SAVINGS_AGENT_PITCH,
  SAVINGS_BUTTON_SX,
  SAVINGS_COLOR,
  SAVINGS_CTA_LABEL,
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
      ? ", mostly from idle cores"
      : ` across ${issuesCount} issue${issuesCount === 1 ? "" : "s"}`;

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
      <Box>
        <Typography variant="body1">
          <b style={{ color: SAVINGS_TEXT_COLOR }}>
            ~{formatSavingsPercentage(savings.totalPercentage)}%
          </b>{" "}
          of this app's compute is recoverable{issuesText}.
        </Typography>
        <Typography variant="body2" color="text.secondary">
          {SAVINGS_AGENT_PITCH}
        </Typography>
      </Box>
      <Button
        size="small"
        variant="contained"
        startIcon={<CodeIcon />}
        sx={SAVINGS_BUTTON_SX}
      >
        {SAVINGS_CTA_LABEL}
      </Button>
    </Box>
  );
};

export default SavingsBanner;
