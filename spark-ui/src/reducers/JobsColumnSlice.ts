import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { HeadCell } from "../components/SqlTable/TableTypes";

interface JobsColumnState {
  headCells: HeadCell[];
  visibleColumns: string[];
}

const initialState: JobsColumnState = {
  headCells: [
    {
      id: "id",
      numeric: true,
      disablePadding: false,
      label: "id",
      initiallyVisible: true,
    },
    {
      id: "status",
      numeric: false,
      disablePadding: false,
      label: "Status",
      initiallyVisible: true,
    },
    {
      id: "description",
      numeric: false,
      disablePadding: false,
      label: "Description",
      initiallyVisible: true,
    },
    {
      id: "duration",
      numeric: false,
      disablePadding: false,
      label: "Duration",
      initiallyVisible: true,
    },
    {
      id: "dcu",
      numeric: false,
      disablePadding: false,
      label: "DCU",
      initiallyVisible: true,
    },
    {
      id: "input",
      numeric: false,
      disablePadding: false,
      label: "Input",
      initiallyVisible: true,
    },
    {
      id: "output",
      numeric: false,
      disablePadding: false,
      label: "Output",
      initiallyVisible: true,
    },
    {
      id: "spill",
      numeric: true,
      disablePadding: false,
      label: "Spill",
      initiallyVisible: false,
    },
    {
      id: "idleCores",
      numeric: true,
      disablePadding: false,
      label: "Idle Cores",
      initiallyVisible: false,
    },
    {
      id: "totalTasks",
      numeric: true,
      disablePadding: false,
      label: "Tasks",
      initiallyVisible: false,
    },
    {
      id: "shuffleReadBytes",
      numeric: true,
      disablePadding: false,
      label: "Shuffle Read",
      initiallyVisible: false,
    },
    {
      id: "shuffleWriteBytes",
      numeric: true,
      disablePadding: false,
      label: "Shuffle Write",
      initiallyVisible: false,
    },
    {
      id: "executorRunTime",
      numeric: true,
      disablePadding: false,
      label: "Executor Run Time",
      initiallyVisible: false,
    }
  ],
  visibleColumns: [],
};

const jobsColumnSlice = createSlice({
  name: "jobsColumns",
  initialState,
  reducers: {
    setVisibleColumns(state, action: PayloadAction<string[]>) {
      state.visibleColumns = action.payload;
    },
    initializeVisibleColumns(state, action: PayloadAction<string[]>) {
      state.visibleColumns = state.headCells
        .filter((cell) => !action.payload.includes(cell.id) && cell.initiallyVisible)
        .map((cell) => cell.id);
    },
    removeVisibleColumn(state, action: PayloadAction<string>) {
      state.visibleColumns = state.visibleColumns.filter(
        (col) => col !== action.payload
      ) || [];
    },
  },
});

export const { setVisibleColumns, initializeVisibleColumns, removeVisibleColumn } = jobsColumnSlice.actions;
export default jobsColumnSlice.reducer;