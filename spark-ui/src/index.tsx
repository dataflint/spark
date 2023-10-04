import * as React from 'react';
import * as ReactDOM from 'react-dom/client';
import CssBaseline from '@mui/material/CssBaseline';
import { ThemeProvider } from '@mui/material/styles';
import theme from './theme';
import { RouterProvider } from 'react-router-dom';
import { reactRouter } from './Router';
import mixpanel from 'mixpanel-browser';
import { MixpanelEvents } from './interfaces/Mixpanel';


const rootElement = document.getElementById('root');
const root = ReactDOM.createRoot(rootElement!);

// If you want to debug mixpanel tracking in dev env uncomment the following line
// const MIX_PANEL_TOKEN = process.env.NODE_ENV === 'development' ? '4b4ba202495eacfd7b46b1147d27f930' : '5251dfa36e60af653d1c6380ccf97857';

const MIX_PANEL_TOKEN = process.env.NODE_ENV === 'development' ? 'dummy_token' : '5251dfa36e60af653d1c6380ccf97857';

mixpanel.init(MIX_PANEL_TOKEN, { track_pageview: true, persistence: 'localStorage' });

mixpanel.track(MixpanelEvents.AppLoaded);

root.render(
  <ThemeProvider theme={theme}>
    {/* CssBaseline kickstart an elegant, consistent, and simple baseline to build upon. */}
    <CssBaseline />
    <RouterProvider router={reactRouter} />
  </ThemeProvider>,
);
