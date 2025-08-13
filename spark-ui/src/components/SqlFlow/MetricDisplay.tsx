import { Box, Tooltip, Typography } from "@mui/material";
import React from "react";
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

const MetricDisplay: React.FC<MetricDisplayProps> = ({ metrics }) => {
    return (
        <Box className={styles.nodeContent}>
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
                        className={styles.metricItem}
                        sx={
                            metric.showBlock
                                ? { justifyContent: "center", alignItems: "center", flexDirection: "column" }
                                : { display: "flex", alignItems: "flex-start", flexDirection: "row" }
                        }
                    >
                        <Typography className={styles.metricName} variant="body2">
                            {metric.name}:
                        </Typography>
                        <Typography className={styles.metricValue} variant="body2">
                            {metric.value}
                        </Typography>
                    </Box>
                </ConditionalWrapper>
            ))}
        </Box>
    );
};

export default MetricDisplay;
