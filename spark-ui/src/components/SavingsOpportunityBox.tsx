import CodeIcon from "@mui/icons-material/Code";
import PaidOutlinedIcon from "@mui/icons-material/PaidOutlined";
import { Box, Grid, Paper, Tooltip, Typography } from "@mui/material";
import React, { FC } from "react";
import { SavingsOpportunity } from "../interfaces/AppStore";
import {
  formatSavingsPercentage,
  openBookDemo,
  SAVINGS_COLOR,
  SAVINGS_TEXT_COLOR,
} from "../utils/BookDemo";

const SavingsOpportunityBox: FC<{ savings: SavingsOpportunity }> = ({
  savings,
}): JSX.Element => {
  const tooltip = (
    <React.Fragment>
      <Typography variant="h6" color="inherit">
        Estimated savings: ~{formatSavingsPercentage(savings.totalPercentage)}%
      </Typography>
      <Typography variant="subtitle2">
        Idle cores: ~{formatSavingsPercentage(savings.idleCoresPercentage)}%
      </Typography>
      <Typography variant="subtitle2">
        Query fixes: ~{formatSavingsPercentage(savings.queriesPercentage)}%
      </Typography>
      <Typography variant="subtitle2">
        Over-provisioned memory: ~
        {formatSavingsPercentage(savings.memoryPercentage)}%
      </Typography>
      <Typography variant="subtitle2" sx={{ mt: 1 }}>
        A rough estimate of this app's compute cost (DCU) that is recoverable.
        DataFlint's agent pinpoints the code changes in your repo and opens the
        PR. Works in Cursor, Claude Code and VS Code via MCP. Click to see it
        live with the DataFlint team.
      </Typography>
    </React.Fragment>
  );

  return (
    <Grid item lg={2}>
      <Tooltip title={tooltip}>
        <Paper
          onClick={() => openBookDemo("summary-tile", savings.totalPercentage)}
          sx={{
            p: 2,
            height: 110,
            display: "flex",
            flexDirection: "column",
            position: "relative",
            cursor: "pointer",
            border: `1px solid ${SAVINGS_COLOR}`,
            background:
              "linear-gradient(135deg, rgba(46,125,50,0.18), rgba(46,125,50,0.04))",
            transition: "transform 0.15s, box-shadow 0.15s",
            "&:hover": {
              transform: "translateY(-2px)",
              boxShadow: `0 4px 16px rgba(46,125,50,0.35)`,
            },
          }}
        >
          {/* same title and value layout as InfoBox, so the tile lines up with its neighbours */}
          <Typography
            component="h2"
            variant="h6"
            color={SAVINGS_TEXT_COLOR}
            gutterBottom
            display="flex"
            justifyContent="center"
            alignItems="center"
            noWrap
          >
            Savings
            <PaidOutlinedIcon sx={{ ml: 1 }} />
          </Typography>
          <Typography
            component="p"
            variant="h4"
            display="flex"
            justifyContent="center"
            alignItems="center"
          >
            ~{formatSavingsPercentage(savings.totalPercentage)}%
          </Typography>
          <Box
            position="absolute"
            left={0}
            right={0}
            bottom={4}
            display="flex"
            justifyContent="center"
            alignItems="center"
            gap={0.5}
            color={SAVINGS_TEXT_COLOR}
          >
            <CodeIcon sx={{ fontSize: 12 }} />
            <Typography variant="caption" fontSize={11} fontWeight={600} noWrap>
              Get the fix →
            </Typography>
          </Box>
        </Paper>
      </Tooltip>
    </Grid>
  );
};

export default SavingsOpportunityBox;
