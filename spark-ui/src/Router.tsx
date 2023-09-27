import React from "react";
import { createHashRouter } from "react-router-dom";
import App from "./App";
import StatusTab from "./tabs/StatusTab";
import ConfigurationTab from "./tabs/ConfigurationTab";
import SummaryTab from "./tabs/SummaryTab";

export const reactRouter = createHashRouter([
  {
    path: "/",
    element: <App />,
    children: [{
      index: true,
      element: <StatusTab />,
    },
    {
      path: "/status",
      element: <StatusTab />,
    },
    {
      path: "/config",
      element: <ConfigurationTab />,
    },
    {
      path: "/summary",
      element: <SummaryTab />,
    },]
  },
]);