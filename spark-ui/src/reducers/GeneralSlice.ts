import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { GraphFilter, SQLNodeExchangeStageData, SQLNodeStageData } from "../interfaces/AppStore";

export const initialState: {
  sqlMode: GraphFilter;
  selectedStage: SQLNodeStageData | SQLNodeExchangeStageData | undefined;
} = {
  sqlMode: "basic",
  selectedStage: undefined
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
  },
});

// Export the action creators and the reducer
export const { setSQLMode, setSelectedStage } = generalSlice.actions;

export default generalSlice.reducer;
