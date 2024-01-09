import React from "react";
import { createHashRouter } from "react-router-dom";
import App from "./App";
import { AlertsTab } from "./tabs/AlertsTab";
import ChatTab from "./tabs/ChatTab";
import ConfigurationTab from "./tabs/ConfigurationTab";
import { ResourcesTab } from "./tabs/ResourcesTab";
import StatusTab from "./tabs/StatusTab";
import SummaryTab from "./tabs/SummaryTab";
import { isHistoryServer } from "./utils/UrlUtils";

const isHistoryServerMode = isHistoryServer();

export const reactRouter = createHashRouter([
  {
    path: "/",
    element: <App />,
    children: [
      {
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
        path: "/alerts",
        element: <AlertsTab />,
      },
      {
        path: "/summary",
        element: <SummaryTab />,
      },
      {
        path: "/sparkassistant",
        element: <ChatTab />,
      },
      {
        path: "/resources",
        element: <ResourcesTab />
      }
    ],
  },
]);
