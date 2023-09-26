import * as React from 'react';
import Box from '@mui/material/Box';
import AppBar from '@mui/material/AppBar';
import Toolbar from '@mui/material/Toolbar';
import SparkAPI from './services/SparkApi';
import { AppStore } from './interfaces/AppStore';
import Drawer from '@mui/material/Drawer';
import List from '@mui/material/List';
import ListItem from '@mui/material/ListItem';
import ListItemText from '@mui/material/ListItemText';
import DevtoolAppBar from './DevtoolAppBar';
import ListItemButton from '@mui/material/ListItemButton';
import ListItemIcon from '@mui/material/ListItemIcon';
import { sparkApiReducer } from './reducers/SparkReducer';
import Progress from './components/Progress';
import { Tab, renderTab, renderTabIcon } from './tabs/TabsSwitcher';
import { AppStateContext } from './Context';
import DisconnectedModal from './components/Modals/DisconnectedModal';
import { initialState } from './reducers/SparkReducer';

const drawerWidth = 240;
let BASE_PATH = "";
if (process.env.NODE_ENV === 'development') {
  BASE_PATH = process.env.REACT_APP_BASE_PATH ?? "";
}

export default function App() {
  const [store, dispatcher] = React.useReducer(sparkApiReducer, initialState);
  const [selectedTab, setSelectedTab] = React.useState(Tab.Status);

  React.useEffect(() => {
    const sparkAPI = new SparkAPI(BASE_PATH, dispatcher)
    const cleanerFunc = sparkAPI.start();
    return cleanerFunc;
  }, []);

  return (
    !store.isInitialized ?
      (
        <Progress />
      )
      :
      (
        <AppStateContext.Provider value={{ isConnected: store.isConnected, isInitialized: store.isInitialized }}>
          <Box sx={{ display: 'flex' }}>
            <AppBar position="fixed" sx={{ zIndex: (theme) => theme.zIndex.drawer + 1 }} color="primary" enableColorOnDark>
              <DevtoolAppBar appName={store.runMetadata.appName ?? ""} />
            </AppBar>
            <Drawer
              variant="permanent"
              sx={{
                width: drawerWidth,
                flexShrink: 0,
                [`& .MuiDrawer-paper`]: { width: drawerWidth, boxSizing: 'border-box' },
              }}
            >
              <DisconnectedModal />
              <Toolbar />
              <Box>
                <List>
                  {Object.values(Tab).map((tab) => (
                    <ListItem key={tab} disablePadding >
                      <ListItemButton selected={selectedTab === tab} onClick={() => setSelectedTab(tab)}>
                        <ListItemIcon>
                          {renderTabIcon(tab)}
                        </ListItemIcon>
                        <ListItemText primary={tab.toString()} />
                      </ListItemButton>
                    </ListItem>
                  ))}
                </List>
              </Box>
            </Drawer>
            <Box
              component="main"
              sx={{
                backgroundColor: (theme) =>
                  theme.palette.mode === 'light'
                    ? theme.palette.grey[100]
                    : theme.palette.grey[900],
                flexGrow: 1,
                height: '100vh',
                overflow: 'hidden',
                pt: '64px'
              }}
            >
              {renderTab(selectedTab, store)}
            </Box>
          </Box>
        </AppStateContext.Provider>
      ))
}
