import {
  ChatContainer,
  MainContainer,
  Message,
  MessageInput,
  MessageList,
  MessageModel,
  TypingIndicator,
} from "@chatscope/chat-ui-kit-react";
import "@chatscope/chat-ui-kit-styles/dist/default/styles.min.css";
import { Visibility, VisibilityOff } from "@mui/icons-material";
import {
  Alert,
  FormControl,
  IconButton,
  Input,
  InputAdornment,
  InputLabel,
  Snackbar,
} from "@mui/material";
import * as React from "react";
import { useAppDispatch, useAppSelector } from "../Hooks";
import { SparkSQLStore } from "../interfaces/AppStore";
import {
  addMessage,
  setApiKey,
  setInputText,
  setIsTyping,
} from "../reducers/ChatSlice";
import { MixpanelService } from "../services/MixpanelService";

function formatToJsonKeyValue(sql: SparkSQLStore): string {
  let result = "";

  sql.sqls.forEach((sql) => {
    result += `ID: ${sql.id}\n`;
    result += `Status: ${sql.status}\n`;
    result += `Description: ${sql.description}\n`;
    result += `Duration: ${sql.duration}\n`;

    if (sql.stageMetrics) {
      result += "Stage Metrics:\n";
      Object.entries(sql.stageMetrics).forEach(([key, value]) => {
        result += `  ${key}: ${value}\n`;
      });
    }

    if (sql.resourceMetrics) {
      result += "Resource Metrics:\n";
      Object.entries(sql.resourceMetrics).forEach(([key, value]) => {
        result += `  ${key}: ${value}\n`;
      });
    }

    const nodeNames = sql.nodes
      .filter((node) => node.type !== "output")
      .map((node: any) => node.nodeName)
      .join(", ");
    result += `Nodes: ${nodeNames}\n\n`;
  });

  return result;
}

export default function ChatTab() {
  const dispatch = useAppDispatch();
  const messages = useAppSelector((state) => state.chat.messages);
  const isTyping = useAppSelector((state) => state.chat.isTyping);
  const apiKey = useAppSelector((state) => state.chat.apiKey);
  const inputText = useAppSelector((state) => state.chat.inputText);
  const sql = useAppSelector((state) => state.spark.sql);
  const status = useAppSelector((state) => state.spark.status);

  const [showPassword, setShowPassword] = React.useState(false);

  const handleClickShowPassword = () => setShowPassword((show) => !show);

  const handleMouseDownPassword = (
    event: React.MouseEvent<HTMLButtonElement>,
  ) => {
    event.preventDefault();
  };

  const [open, setOpen] = React.useState(false);

  const handleClick = () => {
    setOpen(true);
  };

  const handleClose = (
    event?: React.SyntheticEvent | Event,
    reason?: string,
  ) => {
    if (reason === "clickaway") {
      return;
    }

    setOpen(false);
  };

  React.useEffect(() => {
    if (apiKey !== undefined) {
      localStorage.setItem("openaikey", apiKey);
    }
  }, [apiKey]);

  React.useEffect(() => {
    const apiKey = localStorage.getItem("openaikey");
    if (apiKey) {
      dispatch(setApiKey({ apiKey: apiKey }));
    }
  }, []);

  React.useEffect(() => {
    MixpanelService.TrackPageView();
  }, []);

  const handleSendRequest = async (message: string) => {
    const newMessage: MessageModel = {
      message: message,
      sentTime: "just now",
      direction: "incoming",
      sender: "user",
      position: "normal",
    };

    dispatch(setInputText({ inputText: "" }));
    dispatch(addMessage({ message: newMessage }));
    dispatch(setIsTyping({ isTyping: true }));

    try {
      const response = await processMessageToChatGPT([...messages, newMessage]);
      const content = response.choices[0]?.message?.content;
      if (content) {
        const chatGPTResponse: MessageModel = {
          payload: content,
          sender: "Flint Assistant",
          sentTime: "just now",
          position: "normal",
          direction: "outgoing",
        };
        dispatch(addMessage({ message: chatGPTResponse }));
      }
    } catch (error) {
      setOpen(true);
      console.error("Error processing message:", error);
    } finally {
      dispatch(setIsTyping({ isTyping: false }));
    }
  };

  async function processMessageToChatGPT(chatMessages: MessageModel[]) {
    const apiMessages = chatMessages.map((messageObject) => {
      const role =
        messageObject.sender === "Flint Assistant" ? "assistant" : "user";
      return { role, content: messageObject.message };
    });

    const apiRequestBody = {
      model: "gpt-4-1106-preview", // gpt-3.5-turbo
      messages: [
        {
          role: "system",
          content: `You are an AI assistant for helping with analyzing an Apache Spark job. The resulted answers is presented as HTML so please, instead of ** for bold use <strong> and so on`,
        },
        {
          role: "system",
          content: `here is a json that describe metrics of the spark job run: ${JSON.stringify(
            status,
          )}`,
        },
        {
          role: "system",
          content: `here is a key: value representations that describe the SQL queries of the spark job run: ${
            sql ? formatToJsonKeyValue(sql) : "no sql data"
          }`,
        },
        ...apiMessages,
      ],
    };

    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        Authorization: "Bearer " + apiKey,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(apiRequestBody),
    });

    return response.json();
  }

  return (
    <div>
      <div
        style={{
          display: "flex",
          marginTop: "20px",
          justifyContent: "center",
          alignItems: "center",
        }}
      >
        <Snackbar open={open} autoHideDuration={6000} onClose={handleClose}>
          <Alert onClose={handleClose} severity="error" sx={{ width: "100%" }}>
            Flint Assistant Error
          </Alert>
        </Snackbar>
        <div style={{ position: "relative", height: "700px", width: "700px" }}>
          <MainContainer>
            <ChatContainer>
              <MessageList
                scrollBehavior="smooth"
                typingIndicator={
                  isTyping ? (
                    <TypingIndicator content="Flint Assistant is typing" />
                  ) : null
                }
              >
                {messages.map((message, i) => {
                  return <Message key={i} model={message} />;
                })}
              </MessageList>
              <MessageInput
                value={inputText}
                onChange={(innerHtml, text) =>
                  dispatch(setInputText({ inputText: text }))
                }
                disabled={apiKey === undefined}
                placeholder={
                  apiKey === undefined
                    ? "Set the API key to start chatting"
                    : "Ask Flint Assistant anything..."
                }
                attachButton={false}
                onSend={handleSendRequest}
              />
            </ChatContainer>
          </MainContainer>
        </div>
      </div>
      <div
        style={{
          display: "flex",
          justifyContent: "center",
          alignItems: "center",
        }}
      >
        <FormControl sx={{ width: "550px", margin: "20px" }} variant="standard">
          <InputLabel htmlFor="standard-adornment-password">API Key</InputLabel>
          <Input
            id="standard-adornment-password"
            value={apiKey === undefined ? "" : apiKey}
            onChange={(event) =>
              dispatch(setApiKey({ apiKey: event.target.value }))
            }
            type={showPassword ? "text" : "password"}
            endAdornment={
              <InputAdornment position="end">
                <IconButton
                  aria-label="toggle api key visibility"
                  onClick={handleClickShowPassword}
                  onMouseDown={handleMouseDownPassword}
                >
                  {showPassword ? <VisibilityOff /> : <Visibility />}
                </IconButton>
              </InputAdornment>
            }
          />
        </FormControl>
      </div>
    </div>
  );
}
