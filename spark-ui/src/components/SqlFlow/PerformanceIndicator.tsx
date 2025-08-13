import { Box } from "@mui/material";
import React from "react";
import styles from "./node-style.module.css";

interface PerformanceIndicatorProps {
    durationPercentage?: number;
}

const getPerformanceLevel = (percentage: number) => {
    if (percentage <= 30) {
        return {
            className: styles.performanceGood,
            label: "Good Performance"
        };
    } else if (percentage <= 70) {
        return {
            className: styles.performanceWarning,
            label: "Average Performance"
        };
    } else {
        return {
            className: styles.performanceError,
            label: "Attention Needed"
        };
    }
};

const PerformanceIndicator: React.FC<PerformanceIndicatorProps> = ({ durationPercentage }) => {
    if (durationPercentage === undefined) {
        return null;
    }

    const { className, label } = getPerformanceLevel(durationPercentage);

    return (
        <Box
            className={`${styles.performanceBar} ${className}`}
            sx={{ width: `${Math.min(durationPercentage, 100)}%` }}
            aria-label={`${label}: ${durationPercentage.toFixed(1)}%`}
        />
    );
};

export default PerformanceIndicator;
