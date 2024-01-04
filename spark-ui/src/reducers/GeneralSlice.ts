import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { GraphFilter } from "../interfaces/AppStore";

export const initialState: {
  sqlMode: GraphFilter;
} = {
  sqlMode: "basic",
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
  },
});

// Export the action creators and the reducer
export const { setSQLMode } = generalSlice.actions;

export default generalSlice.reducer;
