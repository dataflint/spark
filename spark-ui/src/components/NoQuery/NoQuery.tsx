import { Alert } from "@mui/material";
import React from "react";

const NoQuery = () => {
  return <Alert severity="info">No Spark SQL query currently running</Alert>;
};

export default NoQuery;
