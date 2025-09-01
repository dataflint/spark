import { Box } from "@mui/material";
import React from "react";
import styles from "./node-style.module.css";

interface PerformanceIndicatorProps {
    durationPercentage?: number;
}

const getBucketedColor = (percentage: number): string => {
    if (percentage > 100) {
        percentage = 100;
    }

    const bucket = Math.floor(percentage / 10);

    switch (bucket) {
        case 0:
            return "#4caf50"; // Green
        case 1:
            return "#8bc34a"; // Light green
        case 2:
            return "#ffc107"; // Amber (more readable than lime)
        case 3:
            return "#ff9800"; // Orange (more readable than yellow)
        case 4:
        case 5:
        case 6:
        case 7:
        case 8:
        case 9:
        case 10:
            return "#ff5722"; // Deep orange
        default:
            return "#f44336"; // Red
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
