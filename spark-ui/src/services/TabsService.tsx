import React from "react";
import AdjustIcon from '@mui/icons-material/Adjust';
import SettingsApplicationsIcon from '@mui/icons-material/SettingsApplications';
import AssessmentIcon from '@mui/icons-material/Assessment';

export enum Tab {
  Status = "Status",
  Summary = "Summary",
  Configuration = "Configuration",
}

export const TabToUrl = {
  [Tab.Status]: '/status',
  [Tab.Summary]: '/summary',
  [Tab.Configuration]: '/config',
}

export const getTabByUrl = (path: string) => {
  switch (path) {
    case TabToUrl[Tab.Summary]:
      return Tab.Summary
    case TabToUrl[Tab.Configuration]:
      return Tab.Configuration
    default:
      return Tab.Status;
  }
}


export function renderTabIcon(selectedTab: Tab): JSX.Element {
  switch (selectedTab) {
    case Tab.Status:
      return <AdjustIcon />;
    case Tab.Configuration:
      return <SettingsApplicationsIcon />;
    case Tab.Summary:
      return <AssessmentIcon />;
    default:
      return <div></div>;
  }
}