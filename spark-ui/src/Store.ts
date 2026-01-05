import { configureStore } from "@reduxjs/toolkit";
import ChatSlice from "./reducers/ChatSlice";
import GeneralSlice from "./reducers/GeneralSlice";
import jobsColumnsReducer from "./reducers/JobsColumnSlice";
import SparkSlice from "./reducers/SparkSlice";

const store = configureStore({
  reducer: {
    spark: SparkSlice,
    chat: ChatSlice,
    general: GeneralSlice,
    jobsColumns: jobsColumnsReducer,
  },
  // Disable expensive dev middleware that causes slowdowns with large state (800+ SQL queries)
  middleware: (getDefaultMiddleware) =>
    getDefaultMiddleware({
      serializableCheck: false,
      immutableCheck: false,
    }),
});

// Infer the `RootState` and `AppDispatch` types from the store itself
export type RootState = ReturnType<typeof store.getState>;
// Inferred type: {posts: PostsState, comments: CommentsState, users: UsersState}
export type AppDispatch = typeof store.dispatch;

export default store;
