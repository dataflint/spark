import CodeIcon from "@mui/icons-material/Code";
import { Box, Button, IconButton, Tooltip, Typography } from "@mui/material";
import React, { FC } from "react";
import { useAppSelector } from "../../Hooks";
import {
  formatSavingsPercentage,
  openBookDemo,
  SAVINGS_AGENT_PITCH,
  SAVINGS_COLOR,
  SAVINGS_TEXT_COLOR,
  shouldShowSavingsCta,
} from "../../utils/BookDemo";

// Quiet "get the code fix" card in the empty space under the drawer's tabs, visible on every page
const DrawerSavingsCard: FC<{ open: boolean }> = ({
  open,
}): JSX.Element | null => {
  const savings = useAppSelector(
    (state) => state.spark.alerts?.savingsOpportunity,
  );
  if (savings === undefined || !shouldShowSavingsCta()) {
    return null;
  }
  const savingsText = `~${formatSavingsPercentage(savings.totalPercentage)}%`;
  const onClick = () => openBookDemo("sidebar", savings.totalPercentage);

  if (!open) {
    return (
      <Box display="flex" justifyContent="center" mb={1}>
        <Tooltip
          title={`Get the code fix: ${savingsText} of this app's compute looks recoverable`}
          placement="right"
        >
          <IconButton onClick={onClick} sx={{ color: SAVINGS_TEXT_COLOR }}>
            <CodeIcon />
          </IconButton>
        </Tooltip>
      </Box>
    );
  }

  return (
    <Box
      sx={{
        mx: 1.5,
        mb: 1,
        p: 1.5,
        whiteSpace: "normal",
        borderRadius: 2,
        border: "1px solid rgba(46,125,50,0.5)",
        backgroundColor: "rgba(46,125,50,0.08)",
      }}
    >
      <Typography
        variant="subtitle2"
        color={SAVINGS_TEXT_COLOR}
        display="flex"
        alignItems="center"
        gap={0.75}
        fontWeight={600}
      >
        <CodeIcon fontSize="small" />
        Get the code fix
      </Typography>
      <Typography variant="body2" sx={{ mt: 0.75 }}>
        <b style={{ color: SAVINGS_TEXT_COLOR }}>{savingsText}</b> of this app's
        compute looks recoverable.
      </Typography>
      <Typography
        variant="caption"
        component="p"
        color="text.secondary"
        sx={{ mt: 0.5 }}
      >
        {SAVINGS_AGENT_PITCH}
      </Typography>
      <Button
        size="small"
        variant="outlined"
        fullWidth
        onClick={onClick}
        sx={{
          mt: 1.25,
          textTransform: "none",
          fontWeight: 600,
          color: SAVINGS_TEXT_COLOR,
          borderColor: SAVINGS_COLOR,
          "&:hover": {
            borderColor: SAVINGS_TEXT_COLOR,
            backgroundColor: "rgba(46,125,50,0.12)",
          },
        }}
      >
        See it live →
      </Button>
      <Typography
        variant="caption"
        component="p"
        color="text.secondary"
        textAlign="center"
        sx={{ mt: 0.5, fontSize: 11 }}
      >
        Live walkthrough with the DataFlint team
      </Typography>
    </Box>
  );
};

export default DrawerSavingsCard;
