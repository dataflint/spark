import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import { Box, IconButton, Tooltip } from "@mui/material";
import { duration } from "moment";
import React from "react";
import { useAppDispatch } from "../../Hooks";
import { SQLNodeExchangeStageData, SQLNodeStageData } from "../../interfaces/AppStore";
import { setSelectedStage } from "../../reducers/GeneralSlice";
import { humanizeTimeDiff } from "../../utils/FormatUtils";
import StageIcon from "./StageIcon";
import styles from "./node-style.module.css";

interface NodeFooterProps {
    stage: SQLNodeStageData | SQLNodeExchangeStageData | undefined;
    duration?: number;
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

const NodeFooter: React.FC<NodeFooterProps> = ({
    stage,
    duration: nodeDuration,
    durationPercentage
}) => {
    const dispatch = useAppDispatch();

    const hasStageData = stage && (
        (stage.type === 'onestage' && stage.stageId !== -1) ||
        (stage.type === 'exchange' && (stage.readStage !== -1 || stage.writeStage !== -1))
    );

    return (
        <Box className={styles.nodeFooter}>
            <Box className={styles.footerLeft}>
                <StageIcon stage={stage} />
            </Box>

            <Box className={styles.footerCenter}>
                {durationPercentage !== undefined && (
                    <Tooltip
                        title={
                            nodeDuration !== undefined
                                ? humanizeTimeDiff(duration(nodeDuration))
                                : "Duration information"
                        }
                        arrow
                    >
                        <Box
                            className={styles.durationBadge}
                            sx={{
                                border: `1px solid ${getBucketedColor(durationPercentage)}`,
                                color: getBucketedColor(durationPercentage)
                            }}
                        >
                            {durationPercentage.toFixed(1)}%
                        </Box>
                    </Tooltip>
                )}
            </Box>

            <Box className={styles.footerRight}>
                {hasStageData && (
                    <Tooltip title="View stage details" arrow>
                        <IconButton
                            className={styles.expandButton}
                            size="small"
                            onClick={() => dispatch(setSelectedStage({ selectedStage: stage }))}
                            aria-label="View stage details"
                        >
                            <ExpandMoreIcon sx={{ fontSize: 18 }} />
                        </IconButton>
                    </Tooltip>
                )}
            </Box>
        </Box>
    );
};

export default NodeFooter;
