import {
    Radio as BroadcastIcon,
    Input as InputIcon,
    CallMerge as JoinIcon,
    MoreHoriz as OtherIcon,
    Output as OutputIcon,
    Shuffle as ShuffleIcon,
    Sort as SortIcon,
    TrendingFlat as TransformIcon
} from "@mui/icons-material";
import { Box } from "@mui/material";
import React from "react";
import { NodeType } from "../../interfaces/AppStore";
import styles from "./node-style.module.css";

interface NodeTypeIndicatorProps {
    nodeType: NodeType;
    nodeName: string;
}

const getNodeTypeInfo = (nodeType: NodeType) => {
    switch (nodeType) {
        case "input":
            return { icon: InputIcon, label: "Input Node", color: "#3f51b5" }; // Changed from green to blue
        case "output":
            return { icon: OutputIcon, label: "Output Node", color: "#9c27b0" }; // Changed from red to purple
        case "transformation":
            return { icon: TransformIcon, label: "Transformation Node", color: "#2196f3" };
        case "join":
            return { icon: JoinIcon, label: "Join Node", color: "#ff9800" };
        case "shuffle":
            return { icon: ShuffleIcon, label: "Shuffle Node", color: "#8e24aa" }; // Changed to purple
        case "broadcast":
            return { icon: BroadcastIcon, label: "Broadcast Node", color: "#00bcd4" };
        case "sort":
            return { icon: SortIcon, label: "Sort Node", color: "#795548" };
        default:
            return { icon: OtherIcon, label: "Other Node", color: "#607d8b" };
    }
};

const NodeTypeIndicator: React.FC<NodeTypeIndicatorProps> = ({ nodeType, nodeName }) => {
    const { icon: IconComponent, label, color } = getNodeTypeInfo(nodeType);

    return (
        <Box
            className={styles.nodeTypeIndicator}
            sx={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                width: 28, // Increased size
                height: 28, // Increased size
                borderRadius: '50%',
                backgroundColor: 'rgba(255, 255, 255, 0.9)',
                border: `1px solid ${color}`,
            }}
            aria-label={`${label}: ${nodeName}`}
        >
            <IconComponent
                sx={{
                    fontSize: 18, // Increased icon size
                    color: color
                }}
            />
        </Box>
    );
};

export default NodeTypeIndicator;
