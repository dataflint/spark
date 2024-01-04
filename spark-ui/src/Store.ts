import { configureStore } from "@reduxjs/toolkit";
import ChatSlice from "./reducers/ChatSlice";
import GeneralSlice from "./reducers/GeneralSlice";
import SparkSlice from "./reducers/SparkSlice";

const store = configureStore({
  reducer: {
    spark: SparkSlice,
    chat: ChatSlice,
    general: GeneralSlice,
  },
});

// Infer the `RootState` and `AppDispatch` types from the store itself
export type RootState = ReturnType<typeof store.getState>;
// Inferred type: {posts: PostsState, comments: CommentsState, users: UsersState}
export type AppDispatch = typeof store.dispatch;

export default store;
