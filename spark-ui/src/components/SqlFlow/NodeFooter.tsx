import InfoOutlinedIcon from "@mui/icons-material/InfoOutlined";
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

// Exchange node types that should show the Summary button
const EXCHANGE_NODE_TYPES = [
    "Exchange",
    "BroadcastExchange",
    "GpuBroadcastExchange",
    "GpuColumnarExchange",
    "CometExchange",
];

interface NodeFooterProps {
    stage: SQLNodeStageData | SQLNodeExchangeStageData | undefined;
    duration?: number;
    durationPercentage?: number;
    hideStageDetails?: boolean;
    nodeName?: string;
}



const NodeFooter: React.FC<NodeFooterProps> = ({
    stage,
    duration: nodeDuration,
    durationPercentage,
    hideStageDetails = false,
    nodeName
}) => {
    const dispatch = useAppDispatch();

    // Check if this is an exchange node by stage type
    const isExchangeStage = stage?.type === 'exchange';

    // Check if this is an exchange node by name
    const isExchangeNodeByName = nodeName ? EXCHANGE_NODE_TYPES.includes(nodeName) : false;

    // Show stage details button for exchange nodes (which span multiple stages)
    // For exchange stage type: check that readStage or writeStage is valid
    // For exchange nodes by name: show if they have any stage data
    const hasExchangeStageData = isExchangeStage && stage && (
        (stage.readStage !== -1 || stage.writeStage !== -1)
    );

    // For exchange nodes by name that might have onestage type, also show the button
    const hasExchangeNodeStageData = isExchangeNodeByName && stage && !hideStageDetails;

    const showSummaryButton = hasExchangeStageData || hasExchangeNodeStageData;

    // Show stage icon for exchange nodes
    const showStageIcon = stage && (isExchangeStage || isExchangeNodeByName);

    return (
        <Box className={styles.nodeFooter}>
            <Box className={styles.footerLeft}>
                {showStageIcon && <StageIcon stage={stage} />}
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
                {showSummaryButton && (
                    <Tooltip title="View stage summary" arrow>
                        <IconButton
                            size="small"
                            onClick={() => dispatch(setSelectedStage({ selectedStage: stage }))}
                            sx={{
                                backgroundColor: "rgba(25, 118, 210, 0.9)",
                                color: "#fff",
                                width: 24,
                                height: 24,
                                "&:hover": {
                                    backgroundColor: "rgba(25, 118, 210, 1)",
                                },
                            }}
                        >
                            <InfoOutlinedIcon sx={{ fontSize: 16 }} />
                        </IconButton>
                    </Tooltip>
                )}
            </Box>
        </Box>
    );
};

export default NodeFooter;
