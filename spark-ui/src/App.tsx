import Box from "@mui/material/Box";
import * as React from "react";
import { Outlet, useLocation, useNavigate } from "react-router-dom";
import { AppDrawer } from "./components/AppDrawer/AppDrawer";
import Footer from "./components/Footer";
import DisconnectedModal from "./components/Modals/DisconnectedModal";
import Progress from "./components/Progress";
import { useAppDispatch, useAppSelector } from "./Hooks";
import SparkAPI from "./services/SparkApi";
import { getTabByUrl, Tab, TabToUrl } from "./services/TabsService";
import {
  BASE_CURRENT_PAGE,
  BASE_PATH,
  IS_HISTORY_SERVER_MODE,
} from "./utils/UrlConsts";

const DOCUMENT_TITLE_PREFIX = "DataFlint - ";

export default function App() {
  const location = useLocation();
  const navigate = useNavigate();

  const dispatch = useAppDispatch();
  const store = useAppSelector((state) => state.spark);
  const [selectedTab, setSelectedTab] = React.useState(Tab.Status);

  React.useEffect(() => {
    const sparkAPI = new SparkAPI(
      BASE_PATH,
      BASE_CURRENT_PAGE,
      dispatch,
      IS_HISTORY_SERVER_MODE,
    );
    const cleanerFunc = sparkAPI.start();
    return cleanerFunc;
  }, []);

  React.useEffect(() => {
    if (store.runMetadata?.appName) {
      document.title = DOCUMENT_TITLE_PREFIX + store.runMetadata.appName;
    }
  }, [store.runMetadata?.appName]);

  React.useEffect(() => {
    if (!location || !location.pathname) return;

    setSelectedTab(getTabByUrl(location.pathname));
  }, [location]);

  const onTabChanged = (tab: Tab): void => {
    setSelectedTab(tab);
    navigate(TabToUrl[tab]);
  };

  return !store.isInitialized ? (
    <Progress />
  ) : (
    <Box sx={{ display: "flex" }}>
      <DisconnectedModal />
      <AppDrawer
        appBasePath={BASE_CURRENT_PAGE}
        selectedTab={selectedTab}
        onTabChanged={onTabChanged}
      />
      <Box
        component="main"
        sx={{
          backgroundColor: (theme) =>
            theme.palette.mode === "light"
              ? theme.palette.grey[100]
              : theme.palette.grey[900],
          flexGrow: 1,
          height: "100vh",
          overflow: "hidden",
          display: "flex",
          flexDirection: "column",
        }}
      >
        <Box sx={{ flexGrow: 1, overflow: "hidden" }}>
          <Outlet />
        </Box>
        <Footer />
      </Box>
    </Box>
  );
}
