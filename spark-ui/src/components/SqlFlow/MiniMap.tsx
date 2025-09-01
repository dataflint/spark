import React from "react";
import { MiniMap as ReactFlowMiniMap } from "reactflow";
import { useAppSelector } from "../../Hooks";
import { EnrichedSparkSQL } from "../../interfaces/AppStore";

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

// Custom minimap node component with alert icon support
const CustomMiniMapNode = ({
    x,
    y,
    width,
    height,
    color,
    strokeColor,
    strokeWidth,
    borderRadius,
    selected,
    id,
    sparkSQL,
    alerts
}: any) => {
    // Extract node data from the node ID to check for alerts
    // The minimap node ID is just a number, not "node-123" format
    const nodeId = typeof id === 'string' ? parseInt(id) : id;

    // Check if this node has alerts (alerts are already filtered for current SQL)
    const hasAlert = nodeId && alerts?.some(
        (alert: any) => alert.source.sqlNodeId === nodeId
    );

    return (
        <g>
            {/* Main node rectangle */}
            <rect
                x={x}
                y={y}
                width={width}
                height={height}
                fill={color}
                stroke={strokeColor}
                strokeWidth={strokeWidth}
                rx={borderRadius}
                ry={borderRadius}
            />

            {/* Alert icon overlay - show for nodes with alerts */}
            {hasAlert && (
                <g>
                    {/* Alert background circle - larger dark charcoal for better visibility */}
                    <circle
                        cx={x + width / 2}
                        cy={y + height / 2}
                        r={Math.min(width, height) * 0.45}
                        fill="#424242"
                        stroke="#ffffff"
                        strokeWidth={3}
                    />
                    {/* Alert exclamation mark - larger yellow with bold stroke for maximum visibility */}
                    <text
                        x={x + width / 2}
                        y={y + height / 2 + 1}
                        textAnchor="middle"
                        dominantBaseline="middle"
                        fill="#ffeb3b"
                        fontSize={Math.min(width, height) * 0.65}
                        fontWeight="900"
                        fontFamily="Arial, sans-serif"
                        stroke="#ffeb3b"
                        strokeWidth={3}
                        paintOrder="stroke fill"
                    >
                        !
                    </text>
                </g>
            )}
        </g>
    );
};

interface CustomMiniMapProps {
    sparkSQL: EnrichedSparkSQL;
}

const CustomMiniMap: React.FC<CustomMiniMapProps> = ({ sparkSQL }) => {
    const alerts = useAppSelector((state) => state.spark.alerts)?.alerts.filter(
        (alert) => alert.source.type === "sql" && alert.source.sqlId === sparkSQL.id
    );

    // Create the custom node component with closure over sparkSQL and alerts
    const NodeComponent = (props: any) => (
        <CustomMiniMapNode {...props} sparkSQL={sparkSQL} alerts={alerts} />
    );

    return (
        <ReactFlowMiniMap
            nodeColor={(node) => {
                // Color nodes based on duration percentage using getBucketedColor
                const nodeData = node.data?.node;
                if (nodeData?.durationPercentage !== undefined) {
                    const percentage = nodeData.durationPercentage;
                    return getBucketedColor(percentage);
                }
                // Default green for nodes without duration data
                return "#4caf50";
            }}
            nodeStrokeColor="#ffffff"
            nodeStrokeWidth={2}
            nodeBorderRadius={3}
            nodeComponent={NodeComponent}
            position="bottom-left"
            zoomable
            pannable
            style={{
                backgroundColor: "#0f172a",
                borderRadius: "8px",
                border: "1px solid rgba(255, 255, 255, 0.2)",
                boxShadow: "0 4px 12px rgba(0, 0, 0, 0.5)",
            }}
            offsetScale={10}
        />
    );
};

export default CustomMiniMap;
