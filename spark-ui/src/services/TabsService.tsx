import AdjustIcon from "@mui/icons-material/Adjust";
import AssessmentIcon from "@mui/icons-material/Assessment";
import ReportIcon from "@mui/icons-material/Report";
import SettingsApplicationsIcon from "@mui/icons-material/SettingsApplications";
import React from "react";
import { isHistoryServer } from "../utils/UrlUtils";

export enum Tab {
  Status = "Status",
  Summary = "Summary",
  Configuration = "Configuration",
  Alerts = "Alerts",
}

export const TabToUrl = {
  [Tab.Status]: "/status",
  [Tab.Summary]: "/summary",
  [Tab.Configuration]: "/config",
  [Tab.Alerts]: "/alerts",
};

export const getTabByUrl = (path: string) => {
  switch (path) {
    case TabToUrl[Tab.Status]:
      return Tab.Status;
    case TabToUrl[Tab.Summary]:
      return Tab.Summary;
    case TabToUrl[Tab.Configuration]:
      return Tab.Configuration;
    case TabToUrl[Tab.Alerts]:
      return Tab.Alerts;
    default:
      return isHistoryServer() ? Tab.Summary : Tab.Status;
  }
};

export function renderTabIcon(selectedTab: Tab): JSX.Element {
  switch (selectedTab) {
    case Tab.Status:
      return <AdjustIcon />;
    case Tab.Configuration:
      return <SettingsApplicationsIcon />;
    case Tab.Summary:
      return <AssessmentIcon />;
    case Tab.Alerts:
      return <ReportIcon />;
    default:
      return <div></div>;
  }
}
