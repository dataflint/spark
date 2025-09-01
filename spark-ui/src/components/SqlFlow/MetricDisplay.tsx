import { Box, Tooltip, Typography } from "@mui/material";
import React, { memo } from "react";
import { TransperantTooltip } from "../AlertBadge/AlertBadge";
import { ConditionalWrapper } from "../InfoBox/InfoBox";
import styles from "./node-style.module.css";

export interface MetricWithTooltip {
    name: string;
    value: string;
    tooltip?: string | JSX.Element;
    showBlock?: boolean;
    showSyntax?: boolean;
}

interface MetricDisplayProps {
    metrics: MetricWithTooltip[];
}

const MetricDisplayComponent: React.FC<MetricDisplayProps> = ({ metrics }) => {
    const handleWheel = (e: React.WheelEvent) => {
        // Always prevent React Flow from handling wheel events in this area
        e.stopPropagation();

        // Let the browser handle the scrolling naturally
        // Don't prevent default - we want normal scroll behavior
    };

    // Use different styles based on number of metrics
    const isCompact = metrics.length > 7;
    const isMedium = metrics.length >= 5 && metrics.length <= 7;

    const metricItemClass = isCompact
        ? styles.metricItemCompact
        : isMedium
            ? styles.metricItemMedium
            : styles.metricItem;

    const metricNameClass = isCompact
        ? styles.metricNameCompact
        : isMedium
            ? styles.metricNameMedium
            : styles.metricName;

    const metricValueClass = isCompact
        ? styles.metricValueCompact
        : isMedium
            ? styles.metricValueMedium
            : styles.metricValue;

    return (
        <Box
            className={`${styles.nodeContent} nopan nodrag`}
            onWheel={handleWheel}
            sx={{
                display: 'flex',
                flexDirection: 'column',
                flex: 1,
                minHeight: 0, // Important for flex scrolling
                overflowY: 'auto !important',
                overflowX: 'hidden',
                height: '100%',
                maxHeight: '100%'
            }}
        >
            {metrics.map((metric, index) => (
                <ConditionalWrapper
                    key={`${metric.name}-${index}`}
                    condition={metric.tooltip !== undefined}
                    wrapper={(children) =>
                        typeof metric.tooltip === "string" ? (
                            <Tooltip title={metric.tooltip}>{children}</Tooltip>
                        ) : (
                            <TransperantTooltip title={metric.tooltip}>
                                {children}
                            </TransperantTooltip>
                        )
                    }
                >
                    <Box
                        className={metricItemClass}
                        sx={
                            metric.showBlock
                                ? { justifyContent: "center", alignItems: "center", flexDirection: "column" }
                                : { display: "flex", alignItems: "flex-start", flexDirection: "row" }
                        }
                    >
                        <Typography className={metricNameClass} variant="body2">
                            {metric.name}:
                        </Typography>
                        <Typography className={metricValueClass} variant="body2">
                            {metric.value}
                        </Typography>
                    </Box>
                </ConditionalWrapper>
            ))}
        </Box>
    );
};

// Simple memoization - prevents unnecessary re-renders when metrics haven't changed
const MetricDisplay = memo(MetricDisplayComponent);

export default MetricDisplay;
