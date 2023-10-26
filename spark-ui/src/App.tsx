import * as React from "react";
import Box from "@mui/material/Box";
import SparkAPI from "./services/SparkApi";
import Progress from "./components/Progress";
import {
  Tab,
  TabToUrl,
  getTabByUrl,
  renderTabIcon,
} from "./services/TabsService";
import DisconnectedModal from "./components/Modals/DisconnectedModal";
import { useNavigate, Outlet, useLocation } from "react-router-dom";
import {
  getProxyBasePath,
  hrefWithoutEndSlash,
  isHistoryServer,
  isProxyMode,
} from "./utils/UrlUtils";
import { AppDrawer } from "./components/AppDrawer/AppDrawer";
import { useAppDispatch, useAppSelector } from "./Hooks";

const isHistoryServerMode = isHistoryServer();

let BASE_PATH = "";
let BASE_CURRENT_PAGE = hrefWithoutEndSlash();
if (process.env.NODE_ENV === "development") {
  BASE_PATH = process.env.REACT_APP_BASE_PATH ?? "";
  BASE_CURRENT_PAGE = `${BASE_PATH}/dataflint`;
} else if (!isHistoryServerMode && isProxyMode()) {
  BASE_PATH = getProxyBasePath();
}

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
      isHistoryServerMode,
    );
    const cleanerFunc = sparkAPI.start();
    return cleanerFunc;
  }, []);

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
      {/* <AppBar position="fixed" sx={{ zIndex: (theme) => theme.zIndex.drawer + 1 }} color="primary" enableColorOnDark>
              <DevtoolAppBar appName={store.runMetadata.appName ?? ""} />
            </AppBar> */}
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
        }}
      >
        <Outlet />
      </Box>
    </Box>
  );
}
