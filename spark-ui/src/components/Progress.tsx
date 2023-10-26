import React, { FC } from "react";
import "reactflow/dist/style.css";
import { Box, CircularProgress } from "@mui/material";

const Progress: FC = ({}): JSX.Element => {
  return (
    <Box
      display="flex"
      justifyContent="center"
      alignItems="center"
      minHeight="100vh"
    >
      <CircularProgress />
    </Box>
  );
};

export default Progress;
