import Box from "@mui/material/Box";
import * as React from "react";
import { Outlet, useLocation, useNavigate } from "react-router-dom";
import { useAppDispatch, useAppSelector } from "./Hooks";
import { AppDrawer } from "./components/AppDrawer/AppDrawer";
import DisconnectedModal from "./components/Modals/DisconnectedModal";
import Progress from "./components/Progress";
import SparkAPI from "./services/SparkApi";
import { Tab, TabToUrl, getTabByUrl } from "./services/TabsService";
import { BASE_CURRENT_PAGE, BASE_PATH, IS_HISTORY_SERVER_MODE } from "./utils/UrlConsts";

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
        }}
      >
        <Outlet />
      </Box>
    </Box>
  );
}
