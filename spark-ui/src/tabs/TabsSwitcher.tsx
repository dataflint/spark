import { AppStore } from "../interfaces/AppStore";
import StatusTab from "./StatusTab";
import ConfigurationTab from "./ConfigurationTab";
import React from "react";
import AdjustIcon from '@mui/icons-material/Adjust';
import SettingsApplicationsIcon from '@mui/icons-material/SettingsApplications';
import AssessmentIcon from '@mui/icons-material/Assessment';
import SummaryTab from "./SummaryTab";

export enum Tab {
    Status = "Status",
    Summary = "Summary",
    Configuration = "Configuration",
  }
  
export function renderTab(selectedTab: Tab, store: AppStore): JSX.Element {
    if(!store.isInitialized) {
        // shouldn't happen, should be called when store is already initialized
        return <div></div>;
    }
    switch(selectedTab) {
      case Tab.Status:
        return <StatusTab sql={store.sql} status={store.status} />;
      case Tab.Configuration:
        return <ConfigurationTab config={(store.config as Record<string, string>)} />;
      case Tab.Summary:
        return <SummaryTab sql={store.sql} status={store.status} />;
      default:
        return <div></div>;
    }
  }
  
export function renderTabIcon(selectedTab: Tab): JSX.Element {
    switch(selectedTab) {
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
  