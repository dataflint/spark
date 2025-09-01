import { Box } from "@mui/material";
import React from "react";
import styles from "./node-style.module.css";

interface PerformanceIndicatorProps {
    durationPercentage?: number;
}

export const getBucketedColor = (percentage: number): string => {
    if (percentage > 100) {
        percentage = 100;
    }

    const bucket = Math.floor(percentage / 10);

    switch (bucket) {
        case 0:
            return "#4caf50"; // Green
        case 1:
            return "#ff9800"; // Orange (less forgiving than light green)
        case 2:
            return "#fb8c00"; // Dark amber/orange (better contrast for text)
        case 3:
            return "#ff7043"; // Deep orange
        case 4:
            return "#ff5722"; // Red orange
        case 5:
            return "#f4511e"; // Dark red orange
        case 6:
            return "#e53935"; // Light red
        case 7:
        case 8:
        case 9:
        case 10:
            return "#d32f2f"; // Dark red
        default:
            return "#b71c1c"; // Very dark red
    }
};

const getPerformanceLevel = (percentage: number) => {
    const color = getBucketedColor(percentage);
    const bucket = Math.floor(Math.min(percentage, 100) / 10);

    let label: string;
    if (bucket === 0) {
        label = "Excellent Performance";
    } else if (bucket === 1) {
        label = "Good Performance";
    } else if (bucket === 2) {
        label = "Average Performance";
    } else if (bucket === 3) {
        label = "Below Average Performance";
    } else {
        label = "Attention Needed";
    }

    return {
        color: color,
        label: label
    };
};

const PerformanceIndicator: React.FC<PerformanceIndicatorProps> = ({ durationPercentage }) => {
    if (durationPercentage === undefined) {
        return null;
    }

    const { color, label } = getPerformanceLevel(durationPercentage);

    return (
        <Box
            className={styles.performanceBar}
            sx={{
                width: `${Math.min(durationPercentage, 100)}%`,
                background: color
            }}
            aria-label={`${label}: ${durationPercentage.toFixed(1)}%`}
        />
    );
};

export default PerformanceIndicator;
