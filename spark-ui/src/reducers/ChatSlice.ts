import { MessageModel } from "@chatscope/chat-ui-kit-react";
import { createSlice, PayloadAction } from "@reduxjs/toolkit";

export const initialState: {
  messages: MessageModel[];
  isTyping: boolean;
  inputText: string;
  apiKey: string | undefined;
} = {
  messages: [
    {
      message: "Hello, ask me anything about your spark job!",
      sentTime: "just now",
      sender: "ChatGPT",
      direction: "outgoing",
      position: "normal",
    },
  ],
  isTyping: false,
  apiKey: undefined,
  inputText: "",
};

const chatSlice = createSlice({
  name: "chat",
  initialState,
  reducers: {
    addMessage: (
      state,
      action: PayloadAction<{
        message: MessageModel;
      }>,
    ) => {
      state.messages.push(action.payload.message);
    },
    setIsTyping: (
      state,
      action: PayloadAction<{
        isTyping: boolean;
      }>,
    ) => {
      state.isTyping = action.payload.isTyping;
    },
    setApiKey: (
      state,
      action: PayloadAction<{
        apiKey: string;
      }>,
    ) => {
      state.apiKey =
        action.payload.apiKey === "" ? undefined : action.payload.apiKey;
    },
    setInputText: (
      state,
      action: PayloadAction<{
        inputText: string;
      }>,
    ) => {
      state.inputText = action.payload.inputText;
    },
  },
});

// Export the action creators and the reducer
export const { addMessage, setIsTyping, setApiKey, setInputText } =
  chatSlice.actions;

export default chatSlice.reducer;
