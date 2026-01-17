import BugReportIcon from "@mui/icons-material/BugReport";
import ContentCopyIcon from "@mui/icons-material/ContentCopy";
import CloseIcon from "@mui/icons-material/Close";
import {
  Box,
  Dialog,
  DialogContent,
  DialogTitle,
  IconButton,
  Tooltip,
  Typography,
} from "@mui/material";
import React, { FC, useState } from "react";

interface NodeDebugDialogProps {
  open: boolean;
  onClose: () => void;
  nodeData: any;
  title?: string;
}

const NodeDebugDialog: FC<NodeDebugDialogProps> = ({
  open,
  onClose,
  nodeData,
  title = "Node Debug Info",
}) => {
  const [copied, setCopied] = useState(false);

  const jsonString = JSON.stringify(nodeData, null, 2);

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(jsonString);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch (err) {
      console.error("Failed to copy:", err);
    }
  };

  return (
    <Dialog
      open={open}
      onClose={onClose}
      maxWidth="lg"
      fullWidth
      PaperProps={{
        sx: {
          backgroundColor: "#1e1e1e",
          color: "#d4d4d4",
          borderRadius: 2,
          maxHeight: "85vh",
        },
      }}
    >
      <DialogTitle
        sx={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          borderBottom: "1px solid rgba(255, 255, 255, 0.1)",
          py: 1.5,
          px: 2,
        }}
      >
        <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
          <BugReportIcon sx={{ color: "#4fc3f7", fontSize: 22 }} />
          <Typography variant="h6" sx={{ color: "#fff", fontWeight: 600, fontSize: 16 }}>
            {title}
          </Typography>
        </Box>
        <Box sx={{ display: "flex", gap: 0.5 }}>
          <Tooltip title={copied ? "Copied!" : "Copy JSON"} arrow placement="top">
            <IconButton
              onClick={handleCopy}
              size="small"
              sx={{
                color: copied ? "#4caf50" : "#9e9e9e",
                "&:hover": { color: copied ? "#4caf50" : "#fff" },
              }}
            >
              <ContentCopyIcon fontSize="small" />
            </IconButton>
          </Tooltip>
          <IconButton
            onClick={onClose}
            size="small"
            sx={{ color: "#9e9e9e", "&:hover": { color: "#fff" } }}
          >
            <CloseIcon fontSize="small" />
          </IconButton>
        </Box>
      </DialogTitle>
      <DialogContent sx={{ p: 0 }}>
        <Box
          sx={{
            fontFamily: "'JetBrains Mono', 'Fira Code', 'Monaco', monospace",
            fontSize: 12,
            lineHeight: 1.6,
            overflowX: "auto",
            overflowY: "auto",
            maxHeight: "calc(85vh - 80px)",
            p: 2,
            backgroundColor: "#0d1117",
          }}
        >
          <JsonSyntaxHighlight json={jsonString} />
        </Box>
      </DialogContent>
    </Dialog>
  );
};

// Simple JSON syntax highlighting component
const JsonSyntaxHighlight: FC<{ json: string }> = ({ json }) => {
  const highlightJson = (jsonStr: string): React.ReactNode[] => {
    const lines = jsonStr.split("\n");
    return lines.map((line, lineIndex) => {
      const elements: React.ReactNode[] = [];
      let remaining = line;
      let keyIndex = 0;

      // Match key-value patterns and highlight them
      const regex = /("(?:[^"\\]|\\.)*")\s*(:)?|(\d+(?:\.\d+)?)|(\btrue\b|\bfalse\b|\bnull\b)/g;
      let lastIndex = 0;
      let match;

      while ((match = regex.exec(remaining)) !== null) {
        // Add any text before the match
        if (match.index > lastIndex) {
          elements.push(
            <span key={`${lineIndex}-text-${keyIndex++}`} style={{ color: "#d4d4d4" }}>
              {remaining.slice(lastIndex, match.index)}
            </span>
          );
        }

        if (match[1]) {
          // String - check if it's a key (followed by :) or value
          const isKey = match[2] === ":";
          elements.push(
            <span
              key={`${lineIndex}-str-${keyIndex++}`}
              style={{ color: isKey ? "#9cdcfe" : "#ce9178" }}
            >
              {match[1]}
            </span>
          );
          if (match[2]) {
            elements.push(
              <span key={`${lineIndex}-colon-${keyIndex++}`} style={{ color: "#d4d4d4" }}>
                :
              </span>
            );
          }
        } else if (match[3]) {
          // Number
          elements.push(
            <span key={`${lineIndex}-num-${keyIndex++}`} style={{ color: "#b5cea8" }}>
              {match[3]}
            </span>
          );
        } else if (match[4]) {
          // Boolean or null
          elements.push(
            <span key={`${lineIndex}-bool-${keyIndex++}`} style={{ color: "#569cd6" }}>
              {match[4]}
            </span>
          );
        }

        lastIndex = regex.lastIndex;
      }

      // Add any remaining text
      if (lastIndex < remaining.length) {
        elements.push(
          <span key={`${lineIndex}-end-${keyIndex++}`} style={{ color: "#d4d4d4" }}>
            {remaining.slice(lastIndex)}
          </span>
        );
      }

      return (
        <div key={lineIndex} style={{ whiteSpace: "pre" }}>
          {elements.length > 0 ? elements : " "}
        </div>
      );
    });
  };

  return <>{highlightJson(json)}</>;
};

// Debug button component to use in nodes
interface DebugButtonProps {
  nodeData: any;
  title?: string;
  size?: "small" | "medium";
}

export const DebugButton: FC<DebugButtonProps> = ({ nodeData, title, size = "small" }) => {
  const [dialogOpen, setDialogOpen] = useState(false);

  const handleClick = (e: React.MouseEvent) => {
    e.stopPropagation();
    setDialogOpen(true);
  };

  return (
    <>
      <Tooltip title="Debug: View node JSON" arrow placement="top">
        <IconButton
          onClick={handleClick}
          size={size}
          sx={{
            color: "#78909c",
            backgroundColor: "rgba(255, 255, 255, 0.9)",
            border: "1px solid rgba(0, 0, 0, 0.1)",
            width: size === "small" ? 24 : 28,
            height: size === "small" ? 24 : 28,
            "&:hover": {
              color: "#4fc3f7",
              backgroundColor: "rgba(255, 255, 255, 1)",
            },
          }}
        >
          <BugReportIcon sx={{ fontSize: size === "small" ? 16 : 20 }} />
        </IconButton>
      </Tooltip>
      <NodeDebugDialog
        open={dialogOpen}
        onClose={() => setDialogOpen(false)}
        nodeData={nodeData}
        title={title}
      />
    </>
  );
};

export default NodeDebugDialog;

