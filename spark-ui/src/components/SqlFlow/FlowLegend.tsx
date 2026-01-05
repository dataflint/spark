import CancelIcon from "@mui/icons-material/Cancel";
import ErrorIcon from "@mui/icons-material/Error";
import ExpandLess from "@mui/icons-material/ExpandLess";
import ExpandMore from "@mui/icons-material/ExpandMore";
import PlayArrowIcon from "@mui/icons-material/PlayArrow";
import WarningIcon from "@mui/icons-material/Warning";
import {
    Box,
    Collapse,
    IconButton,
    Paper,
    Tooltip,
    Typography,
} from "@mui/material";
import React, { useState } from "react";
import { getBucketedColor } from "./PerformanceIndicator";

// Status colors matching the task indicators
const STATUS_COLORS = {
    complete: "#4caf50",           // Green
    completeWithRetries: "#ff9800", // Orange
    active: "#1976d2",             // Blue
    failed: "#f44336",             // Red
    pending: "#b2a300",            // Yellow
};

interface LegendItemProps {
    icon: React.ReactNode;
    label: string;
    description?: string;
}

const LegendItem: React.FC<LegendItemProps> = ({ icon, label, description }) => (
    <Tooltip title={description || label} arrow placement="left">
        <Box
            sx={{
                display: "flex",
                alignItems: "center",
                gap: 1,
                py: 0.5,
                px: 1,
                borderRadius: 1,
                "&:hover": {
                    backgroundColor: "rgba(255, 255, 255, 0.1)",
                },
            }}
        >
            <Box sx={{ width: 24, height: 24, display: "flex", alignItems: "center", justifyContent: "center" }}>
                {icon}
            </Box>
            <Typography sx={{ fontSize: 11, color: "rgba(255, 255, 255, 0.9)" }}>
                {label}
            </Typography>
        </Box>
    </Tooltip>
);

interface ColorBarProps {
    color: string;
    label: string;
    description?: string;
}

const ColorBar: React.FC<ColorBarProps> = ({ color, label, description }) => (
    <Tooltip title={description || label} arrow placement="left">
        <Box
            sx={{
                display: "flex",
                alignItems: "center",
                gap: 1,
                py: 0.5,
                px: 1,
                borderRadius: 1,
                "&:hover": {
                    backgroundColor: "rgba(255, 255, 255, 0.1)",
                },
            }}
        >
            <Box
                sx={{
                    width: 24,
                    height: 8,
                    borderRadius: 1,
                    backgroundColor: color,
                }}
            />
            <Typography sx={{ fontSize: 11, color: "rgba(255, 255, 255, 0.9)" }}>
                {label}
            </Typography>
        </Box>
    </Tooltip>
);

interface FlowLegendProps {
    inline?: boolean;
}

const FlowLegend: React.FC<FlowLegendProps> = ({ inline = false }) => {
    const [isExpanded, setIsExpanded] = useState(false);

    return (
        <Box sx={{ position: "relative", alignSelf: "flex-end", minHeight: isExpanded ? 520 : "auto" }}>
            <Paper
                elevation={3}
                sx={{
                    backgroundColor: "rgba(30, 41, 59, 0.95)",
                    backdropFilter: "blur(8px)",
                    borderRadius: 2,
                    overflow: "hidden",
                    minWidth: 140,
                    maxWidth: 220,
                    position: isExpanded ? "absolute" : "relative",
                    bottom: 0,
                    right: 0,
                }}
            >
                {/* Collapsible content - placed BEFORE header so it expands upward */}
                <Collapse in={isExpanded}>
                    <Box sx={{ p: 1 }}>
                        {/* Task Progress Section */}
                        <Typography
                            sx={{
                                fontSize: 10,
                                fontWeight: 600,
                                color: "rgba(255, 255, 255, 0.6)",
                                textTransform: "uppercase",
                                letterSpacing: 0.5,
                                mb: 0.5,
                                px: 1,
                            }}
                        >
                            Task Progress
                        </Typography>
                        <LegendItem
                            icon={
                                <Box sx={{
                                    display: "flex",
                                    alignItems: "center",
                                    gap: 0.5,
                                    px: 0.5,
                                    py: 0.2,
                                    borderRadius: 0.5,
                                    backgroundColor: `${STATUS_COLORS.complete}22`,
                                    border: `1px solid ${STATUS_COLORS.complete}66`,
                                }}>
                                    <Typography sx={{ fontSize: 8, fontWeight: 600, color: STATUS_COLORS.complete }}>5/5</Typography>
                                </Box>
                            }
                            label="Complete"
                            description="All tasks finished successfully"
                        />
                        <LegendItem
                            icon={
                                <Box sx={{
                                    display: "flex",
                                    alignItems: "center",
                                    gap: 0.5,
                                    px: 0.5,
                                    py: 0.2,
                                    borderRadius: 0.5,
                                    backgroundColor: `${STATUS_COLORS.completeWithRetries}22`,
                                    border: `1px solid ${STATUS_COLORS.completeWithRetries}66`,
                                }}>
                                    <Typography sx={{ fontSize: 8, fontWeight: 600, color: STATUS_COLORS.completeWithRetries }}>5/5</Typography>
                                </Box>
                            }
                            label="Complete (Retries)"
                            description="Completed with task retries"
                        />
                        <LegendItem
                            icon={
                                <Box sx={{
                                    display: "flex",
                                    alignItems: "center",
                                    gap: 0.3,
                                    px: 0.5,
                                    py: 0.2,
                                    borderRadius: 0.5,
                                    backgroundColor: `${STATUS_COLORS.active}22`,
                                    border: `1px solid ${STATUS_COLORS.active}66`,
                                }}>
                                    <PlayArrowIcon sx={{ fontSize: 10, color: STATUS_COLORS.active }} />
                                    <Typography sx={{ fontSize: 8, fontWeight: 600, color: STATUS_COLORS.active }}>3/5</Typography>
                                </Box>
                            }
                            label="Active"
                            description="Stage is currently running"
                        />
                        <LegendItem
                            icon={
                                <Box sx={{
                                    display: "flex",
                                    alignItems: "center",
                                    gap: 0.5,
                                    px: 0.5,
                                    py: 0.2,
                                    borderRadius: 0.5,
                                    backgroundColor: `${STATUS_COLORS.pending}22`,
                                    border: `1px solid ${STATUS_COLORS.pending}66`,
                                }}>
                                    <Typography sx={{ fontSize: 8, fontWeight: 600, color: STATUS_COLORS.pending }}>0/5</Typography>
                                </Box>
                            }
                            label="Pending"
                            description="Stage is waiting to start"
                        />
                        <LegendItem
                            icon={
                                <Box sx={{
                                    display: "flex",
                                    alignItems: "center",
                                    gap: 0.3,
                                    px: 0.5,
                                    py: 0.2,
                                    borderRadius: 0.5,
                                    backgroundColor: `${STATUS_COLORS.failed}22`,
                                    border: `1px solid ${STATUS_COLORS.failed}66`,
                                }}>
                                    <Typography sx={{ fontSize: 8, fontWeight: 600, color: STATUS_COLORS.failed }}>2/5</Typography>
                                    <CancelIcon sx={{ fontSize: 10, color: STATUS_COLORS.failed }} />
                                </Box>
                            }
                            label="Failed"
                            description="Stage has failed tasks"
                        />

                        {/* Duration Colors Section */}
                        <Typography
                            sx={{
                                fontSize: 10,
                                fontWeight: 600,
                                color: "rgba(255, 255, 255, 0.6)",
                                textTransform: "uppercase",
                                letterSpacing: 0.5,
                                mt: 1.5,
                                mb: 0.5,
                                px: 1,
                            }}
                        >
                            Duration (% of Total)
                        </Typography>
                        <ColorBar
                            color={getBucketedColor(5)}
                            label="0-10%"
                            description="Excellent performance - minimal time spent"
                        />
                        <ColorBar
                            color={getBucketedColor(15)}
                            label="10-20%"
                            description="Good performance"
                        />
                        <ColorBar
                            color={getBucketedColor(35)}
                            label="20-50%"
                            description="Moderate - may need attention"
                        />
                        <ColorBar
                            color={getBucketedColor(75)}
                            label="50-100%"
                            description="High duration - potential bottleneck"
                        />

                        {/* Minimap Colors Section */}
                        <Typography
                            sx={{
                                fontSize: 10,
                                fontWeight: 600,
                                color: "rgba(255, 255, 255, 0.6)",
                                textTransform: "uppercase",
                                letterSpacing: 0.5,
                                mt: 1.5,
                                mb: 0.5,
                                px: 1,
                            }}
                        >
                            Minimap Colors
                        </Typography>
                        <LegendItem
                            icon={
                                <Box
                                    sx={{
                                        width: 14,
                                        height: 14,
                                        borderRadius: 0.5,
                                        backgroundColor: "#4caf50",
                                        border: "1px solid #fff",
                                    }}
                                />
                            }
                            label="Completed"
                            description="Node completed successfully"
                        />
                        <LegendItem
                            icon={
                                <Box
                                    sx={{
                                        width: 14,
                                        height: 14,
                                        borderRadius: 0.5,
                                        backgroundColor: "#9e9e9e",
                                        border: "1px solid #fff",
                                    }}
                                />
                            }
                            label="Pending"
                            description="Node waiting to start"
                        />
                        <LegendItem
                            icon={
                                <Box
                                    sx={{
                                        width: 14,
                                        height: 14,
                                        borderRadius: 0.5,
                                        backgroundColor: "#1976d2",
                                        border: "1px solid #fff",
                                    }}
                                />
                            }
                            label="Active"
                            description="Node is currently running"
                        />
                        <LegendItem
                            icon={
                                <Box
                                    sx={{
                                        width: 14,
                                        height: 14,
                                        borderRadius: 0.5,
                                        backgroundColor: "#9c27b0",
                                        border: "1px solid #fff",
                                    }}
                                />
                            }
                            label="Failed"
                            description="Node failed with an error"
                        />
                        <LegendItem
                            icon={
                                <Box
                                    sx={{
                                        width: 14,
                                        height: 14,
                                        borderRadius: 0.5,
                                        backgroundColor: "#424242",
                                        border: "1px solid #fff",
                                        display: "flex",
                                        alignItems: "center",
                                        justifyContent: "center",
                                    }}
                                >
                                    <Typography sx={{ color: "#ffeb3b", fontSize: 10, fontWeight: 900, lineHeight: 1 }}>!</Typography>
                                </Box>
                            }
                            label="Has Alert"
                            description="Node has a performance alert"
                        />

                        {/* Alerts Section */}
                        <Typography
                            sx={{
                                fontSize: 10,
                                fontWeight: 600,
                                color: "rgba(255, 255, 255, 0.6)",
                                textTransform: "uppercase",
                                letterSpacing: 0.5,
                                mt: 1.5,
                                mb: 0.5,
                                px: 1,
                            }}
                        >
                            Alerts
                        </Typography>
                        <LegendItem
                            icon={<WarningIcon sx={{ color: "#ff9100", fontSize: 18 }} />}
                            label="Warning"
                            description="Performance warning - optimization suggested"
                        />
                        <LegendItem
                            icon={<ErrorIcon sx={{ color: "#bf360c", fontSize: 18 }} />}
                            label="Error"
                            description="Critical issue detected"
                        />
                    </Box>
                </Collapse>

                {/* Header - always visible at the bottom */}
                <Box
                    onClick={() => setIsExpanded(!isExpanded)}
                    sx={{
                        display: "flex",
                        alignItems: "center",
                        justifyContent: "space-between",
                        px: 1.5,
                        py: 1,
                        cursor: "pointer",
                        borderTop: isExpanded ? "1px solid rgba(255, 255, 255, 0.1)" : "none",
                        "&:hover": {
                            backgroundColor: "rgba(255, 255, 255, 0.05)",
                        },
                    }}
                >
                    <Typography sx={{ fontSize: 12, fontWeight: 600, color: "#fff" }}>
                        Legend
                    </Typography>
                    <IconButton size="small" sx={{ color: "rgba(255, 255, 255, 0.7)", p: 0 }}>
                        {isExpanded ? <ExpandMore fontSize="small" /> : <ExpandLess fontSize="small" />}
                    </IconButton>
                </Box>
            </Paper>
        </Box>
    );
};

export default FlowLegend;
