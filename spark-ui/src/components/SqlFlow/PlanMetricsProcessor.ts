import { truncateMiddle } from "../../reducers/PlanParsers/PlanParserUtils";
import { MetricWithTooltip } from "./MetricDisplay";
import { addTruncatedCodeTooltip, addTruncatedSmallTooltip } from "./MetricProcessors";

// Import the plan types - we'll need to check what the actual import path is
// For now, I'll define the interface locally to avoid import errors
interface ParsedPlan {
    type: string;
    plan: any;
}

class PlanMetricsProcessor {
    static processPlanMetrics(parsedPlan: ParsedPlan): MetricWithTooltip[] {
        const metrics: MetricWithTooltip[] = [];

        switch (parsedPlan.type) {
            case "CollectLimit":
                return this.processCollectLimit(parsedPlan.plan, metrics);

            case "Filter":
                return this.processFilter(parsedPlan.plan, metrics);

            case "Coalesce":
                return this.processCoalesce(parsedPlan.plan, metrics);

            case "TakeOrderedAndProject":
                return this.processTakeOrderedAndProject(parsedPlan.plan, metrics);

            case "WriteToHDFS":
                return this.processWriteToHDFS(parsedPlan.plan, metrics);

            case "WriteToDelta":
                return this.processWriteToDelta(parsedPlan.plan, metrics);

            case "FileScan":
                return this.processFileScan(parsedPlan.plan, metrics);

            case "JDBCScan":
                return this.processJDBCScan(parsedPlan.plan, metrics);

            case "Exchange":
                return this.processExchange(parsedPlan.plan, metrics);

            case "Project":
                return this.processProject(parsedPlan.plan, metrics);

            case "HashAggregate":
                return this.processHashAggregate(parsedPlan.plan, metrics);

            case "Sort":
                return this.processSort(parsedPlan.plan, metrics);

            case "Join":
                return this.processJoin(parsedPlan.plan, metrics);

            case "Window":
                return this.processWindow(parsedPlan.plan, metrics);

            case "BatchEvalPython":
                return this.processBatchEvalPython(parsedPlan.plan, metrics);

            case "Generate":
                return this.processGenerate(parsedPlan.plan, metrics);

            case "Expand":
                return this.processExpand(parsedPlan.plan, metrics);

            default:
                return metrics;
        }
    }

    private static processCollectLimit(plan: any, metrics: MetricWithTooltip[]): MetricWithTooltip[] {
        if (plan.limit !== undefined) {
            metrics.push({
                name: "Limit",
                value: plan.limit.toString(),
            });
        }
        return metrics;
    }

    private static processFilter(plan: any, metrics: MetricWithTooltip[]): MetricWithTooltip[] {
        if (plan.condition) {
            addTruncatedCodeTooltip(metrics, "Condition", plan.condition);
        }
        return metrics;
    }

    private static processCoalesce(plan: any, metrics: MetricWithTooltip[]): MetricWithTooltip[] {
        if (plan.partitionNum !== undefined) {
            metrics.push({
                name: "Partitions",
                value: plan.partitionNum.toString(),
            });
        }
        return metrics;
    }

    private static processTakeOrderedAndProject(plan: any, metrics: MetricWithTooltip[]): MetricWithTooltip[] {
        if (plan.limit !== undefined) {
            metrics.push({
                name: "Limit",
                value: plan.limit.toString(),
            });
        }
        return metrics;
    }

    private static processWriteToHDFS(plan: any, metrics: MetricWithTooltip[]): MetricWithTooltip[] {
        if (plan.tableName !== undefined) {
            addTruncatedSmallTooltip(metrics, "Table Name", plan.tableName);
        }
        if (plan.location) {
            addTruncatedSmallTooltip(metrics, "File Path", plan.location);
        }
        if (plan.format) {
            metrics.push({ name: "Format", value: plan.format });
        }
        if (plan.mode) {
            metrics.push({ name: "Mode", value: plan.mode });
        }
        if (plan.partitionKeys !== undefined && plan.partitionKeys.length > 0) {
            addTruncatedSmallTooltipMultiLine(metrics, "Partition By", plan.partitionKeys);
        }
        return metrics;
    }

    private static processWriteToDelta(plan: any, metrics: MetricWithTooltip[]): MetricWithTooltip[] {
        if (plan.location) {
            addTruncatedSmallTooltip(metrics, "Location", plan.location);
        }
        if (plan.fields !== undefined && plan.fields.length > 0) {
            addTruncatedCodeTooltipMultiline(metrics, "Output Fields", plan.fields);
        }
        return metrics;
    }

    private static processFileScan(plan: any, metrics: MetricWithTooltip[]): MetricWithTooltip[] {
        if (plan.PushedFilters !== undefined && plan.PushedFilters.length > 0) {
            addTruncatedCodeTooltipMultiline(metrics, "Push Down Filters", plan.PushedFilters, 25, false);
        }

        if (plan.PartitionFilters !== undefined) {
            if (plan.PartitionFilters.length > 0) {
                addTruncatedCodeTooltipMultiline(metrics, "Partition Filters", plan.PartitionFilters, 100, false);
            } else {
                metrics.push({
                    name: "Partition Filters",
                    value: "Full Scan",
                });
            }
        }

        if (plan.Location !== undefined) {
            addTruncatedSmallTooltip(metrics, "File Path", plan.Location);
        }

        if (plan.tableName !== undefined) {
            addTruncatedSmallTooltip(metrics, "Table", plan.tableName);
        }

        return metrics;
    }

    private static processJDBCScan(plan: any, metrics: MetricWithTooltip[]): MetricWithTooltip[] {
        // 1. Number of partitions read
        if (plan.numPartitions !== undefined) {
            metrics.push({
                name: "Partitions",
                value: plan.numPartitions.toString(),
            });
        }

        // 2. Full scan table name (if it's a simple table scan)
        if (plan.isFullScan && plan.tableName !== undefined) {
            addTruncatedSmallTooltip(metrics, "Table (Full Scan)", plan.tableName);
        } else if (plan.tableName !== undefined && !plan.isCustomQuery) {
            addTruncatedSmallTooltip(metrics, "Table", plan.tableName);
        }

        // 3. If it's custom SQL - show query as code block (this takes priority)
        if (plan.sql !== undefined) {
            addTruncatedCodeTooltip(metrics, "SQL", plan.sql, 200, false, true);
        }

        // 4. If it's scan with filter - show pushed filters
        if (plan.pushedFilters !== undefined && plan.pushedFilters.length > 0) {
            addTruncatedCodeTooltipMultiline(metrics, "Push Down Filters", plan.pushedFilters, 25, false);
        }

        // 5. If it's scan with select - show selected columns
        if (plan.selectedColumns !== undefined && plan.selectedColumns.length > 0) {
            const columnsStr = plan.selectedColumns.join(", ");
            addTruncatedSmallTooltip(metrics, "Selected Columns", columnsStr, 80, false, false, false);
        }

        return metrics;
    }

    private static processExchange(plan: any, metrics: MetricWithTooltip[]): MetricWithTooltip[] {
        // Delta optimize write is handled in processExchangeMetrics in MetricProcessors.tsx
        // to avoid duplication and ensure it appears prominently
        if (plan.deltaOptimizeWrite !== undefined) {
            return metrics;
        }

        if (plan.fields !== undefined && plan.fields.length > 0) {
            const fieldLabel = plan.type === "hashpartitioning"
                ? (plan.fields.length === 1 ? "Hashed Field" : "Hashed Fields")
                : (plan.fields.length === 1 ? "Ranged Field" : "Ranged Fields");

            addTruncatedCodeTooltipMultiline(metrics, fieldLabel, plan.fields);
        }
        return metrics;
    }

    private static processProject(plan: any, metrics: MetricWithTooltip[]): MetricWithTooltip[] {
        if (plan.fields !== undefined) {
            if (plan.fields.length === 0) {
                metrics.push({
                    name: "Selected Fields",
                    value: "*",
                });
            } else {
                addTruncatedCodeTooltipMultiline(metrics, "Selected Fields", plan.fields);
            }
        }
        return metrics;
    }

    private static processHashAggregate(plan: any, metrics: MetricWithTooltip[]): MetricWithTooltip[] {
        if (plan.keys !== undefined && plan.keys.length > 0) {
            const keyLabel = plan.functions?.length === 0 ? "Distinct By" : "Group By";
            addTruncatedCodeTooltipMultiline(metrics, keyLabel, plan.keys);
        }

        if (plan.functions !== undefined && plan.functions.length > 0) {
            addTruncatedCodeTooltipMultiline(metrics, "Expression", plan.functions);
        }

        return metrics;
    }

    private static processSort(plan: any, metrics: MetricWithTooltip[]): MetricWithTooltip[] {
        if (plan.fields !== undefined && plan.fields.length > 0) {
            addTruncatedSmallTooltipMultiLine(metrics, "Sort By", plan.fields);
        }
        return metrics;
    }

    private static processJoin(plan: any, metrics: MetricWithTooltip[]): MetricWithTooltip[] {
        if (plan.joinSideType) {
            metrics.push({
                name: "Join Type",
                value: plan.joinSideType,
            });
        }

        if (plan.leftKeys !== undefined && plan.leftKeys.length > 0) {
            const keyLabel = plan.leftKeys.length > 1 ? "Left Side Keys" : "Left Side Key";
            metrics.push({
                name: keyLabel,
                value: truncateMiddle(plan.leftKeys.join(", "), 25),
                tooltip: plan.leftKeys.length > 25 ? plan.leftKeys.join(", ") : undefined,
            });
        }

        if (plan.rightKeys !== undefined && plan.rightKeys.length > 0) {
            const keyLabel = plan.rightKeys.length > 1 ? "Right Side Keys" : "Right Side Key";
            metrics.push({
                name: keyLabel,
                value: truncateMiddle(plan.rightKeys.join(", "), 25),
                tooltip: plan.rightKeys.length > 25 ? plan.rightKeys.join(", ") : undefined,
            });
        }

        if (plan.joinCondition !== undefined && plan.joinCondition !== "") {
            addTruncatedCodeTooltip(metrics, "Join Condition", plan.joinCondition);
        }

        return metrics;
    }

    private static processWindow(plan: any, metrics: MetricWithTooltip[]): MetricWithTooltip[] {
        if (plan.partitionFields !== undefined && plan.partitionFields.length > 0) {
            addTruncatedSmallTooltipMultiLine(metrics, "Partition Fields", plan.partitionFields);
        }

        if (plan.sortFields !== undefined && plan.sortFields.length > 0) {
            addTruncatedSmallTooltipMultiLine(metrics, "Sort Fields", plan.sortFields);
        }

        if (plan.selectFields !== undefined && plan.selectFields.length > 0) {
            addTruncatedCodeTooltipMultiline(metrics, "Select Fields", plan.selectFields);
        }

        return metrics;
    }

    private static processBatchEvalPython(plan: any, metrics: MetricWithTooltip[]): MetricWithTooltip[] {
        if (plan.functionNames !== undefined && plan.functionNames.length > 0) {
            addTruncatedCodeTooltipMultiline(metrics, "Functions", plan.functionNames);
        }
        return metrics;
    }

    private static processGenerate(plan: any, metrics: MetricWithTooltip[]): MetricWithTooltip[] {
        if (plan.selectedFields !== undefined && plan.selectedFields.length > 0) {
            addTruncatedSmallTooltipMultiLine(metrics, "Selected Fields", plan.selectedFields);
        }

        if (plan.explodeField !== undefined && plan.explodeField.length > 0) {
            const fieldLabel = plan.explodeField.length === 1
                ? `${plan.operation} Field`
                : `${plan.operation} Fields`;

            if (plan.explodeField.length === 1) {
                addTruncatedSmallTooltip(metrics, fieldLabel, plan.explodeField[0]);
            } else {
                addTruncatedSmallTooltipMultiLine(metrics, fieldLabel, plan.explodeField);
            }
        }

        if (plan.explodeByField !== undefined) {
            addTruncatedSmallTooltip(metrics, `${plan.operation} By`, plan.explodeByField);
        }

        return metrics;
    }

    private static processExpand(plan: any, metrics: MetricWithTooltip[]): MetricWithTooltip[] {
        if (plan.fields !== undefined && plan.fields.length > 0) {
            addTruncatedCodeTooltipMultiline(metrics, "Expanded Fields", plan.fields);
        }

        if (plan.idField !== undefined) {
            addTruncatedSmallTooltip(metrics, "ID Field", plan.idField, 25, true, false, false);
        }

        return metrics;
    }
}

// Helper functions that mirror the original implementations
function addTruncatedSmallTooltipMultiLine(
    metrics: MetricWithTooltip[],
    name: string,
    value: string[],
    limit: number = 25,
    pushEnd: boolean = false,
) {
    addTruncatedSmallTooltip(metrics, name, value.join(", "), limit, pushEnd, true);
}

function addTruncatedCodeTooltipMultiline(
    metrics: MetricWithTooltip[],
    name: string,
    value: string[],
    limit = 120,
    pushEnd: boolean = true,
    showBlock: boolean = true,
) {
    addTruncatedCodeTooltip(metrics, name, value.join(",\n"), limit, pushEnd, showBlock);
}

export default PlanMetricsProcessor;
