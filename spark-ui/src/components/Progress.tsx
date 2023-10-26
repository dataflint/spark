import { Box, CircularProgress } from "@mui/material";
import React, { FC } from "react";
import "reactflow/dist/style.css";

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
