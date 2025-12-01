import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { GraphFilter, SQLNodeExchangeStageData, SQLNodeStageData } from "../interfaces/AppStore";

export const initialState: {
  sqlMode: GraphFilter;
  selectedStage: SQLNodeStageData | SQLNodeExchangeStageData | undefined;
  telemetryEnabled: boolean;
} = {
  sqlMode: "advanced",
  selectedStage: undefined,
  telemetryEnabled: false
};

const generalSlice = createSlice({
  name: "general",
  initialState,
  reducers: {
    setSQLMode: (
      state,
      action: PayloadAction<{
        newMode: GraphFilter;
      }>,
    ) => {
      state.sqlMode = action.payload.newMode;
    },
    setSelectedStage: (
      state,
      action: PayloadAction<{
        selectedStage: SQLNodeStageData | SQLNodeExchangeStageData | undefined;
      }>,
    ) => {
      state.selectedStage = action.payload.selectedStage;
    },
    setTelemetryEnabled: (
      state,
      action: PayloadAction<{
        enabled: boolean;
      }>,
    ) => {
      state.telemetryEnabled = action.payload.enabled;
    },
  },
});

// Export the action creators and the reducer
export const { setSQLMode, setSelectedStage, setTelemetryEnabled } = generalSlice.actions;

export default generalSlice.reducer;
