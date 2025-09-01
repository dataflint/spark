import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import { Box, IconButton, Tooltip } from "@mui/material";
import { duration } from "moment";
import React from "react";
import { useAppDispatch } from "../../Hooks";
import { SQLNodeExchangeStageData, SQLNodeStageData } from "../../interfaces/AppStore";
import { setSelectedStage } from "../../reducers/GeneralSlice";
import { humanizeTimeDiff } from "../../utils/FormatUtils";
import styles from "./node-style.module.css";
import { getBucketedColor } from "./PerformanceIndicator";
import StageIcon from "./StageIcon";

interface NodeFooterProps {
    stage: SQLNodeStageData | SQLNodeExchangeStageData | undefined;
    duration?: number;
    durationPercentage?: number;
}



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
