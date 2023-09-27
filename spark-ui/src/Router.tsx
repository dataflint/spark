import React from "react";
import { createHashRouter } from "react-router-dom";
import App from "./App";
import StatusTab from "./tabs/StatusTab";
import ConfigurationTab from "./tabs/ConfigurationTab";
import SummaryTab from "./tabs/SummaryTab";

let BASE_NAME: string | undefined = "/devtool";
if (process.env.NODE_ENV === 'development') {
  BASE_NAME = undefined;
}

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
], {
  basename: BASE_NAME
});