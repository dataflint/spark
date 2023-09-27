import React from "react";
import { createHashRouter } from "react-router-dom";
import App from "./App";
import StatusTab from "./tabs/StatusTab";
import ConfigurationTab from "./tabs/ConfigurationTab";
import SummaryTab from "./tabs/SummaryTab";
import { isHistoryServer } from "./utils/UrlUtils";


const isHistoryServerMode = isHistoryServer();

export const reactRouter = createHashRouter([
  {
    path: "/",
    element: <App />,
    children: [{
      index: true,
      element: isHistoryServerMode ? <SummaryTab /> : <StatusTab />,
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