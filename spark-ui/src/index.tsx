import * as React from "react";
import * as ReactDOM from "react-dom/client";
import CssBaseline from "@mui/material/CssBaseline";
import { ThemeProvider } from "@mui/material/styles";
import theme from "./theme";
import { RouterProvider } from "react-router-dom";
import { reactRouter } from "./Router";
import { MixpanelEvents } from "./interfaces/Mixpanel";
import { MixpanelService } from "./services/MixpanelService";
import { Provider } from "react-redux";
import store from "./Store";

const rootElement = document.getElementById("root");
const root = ReactDOM.createRoot(rootElement!);

MixpanelService.InitMixpanel();
MixpanelService.Track(MixpanelEvents.AppLoaded);

root.render(
  <Provider store={store}>
    <ThemeProvider theme={theme}>
      {/* CssBaseline kickstart an elegant, consistent, and simple baseline to build upon. */}
      <CssBaseline />
      <RouterProvider router={reactRouter} />
    </ThemeProvider>
  </Provider>,
);
