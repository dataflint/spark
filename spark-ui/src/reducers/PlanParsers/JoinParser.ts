import { ParsedJoinPlan } from "../../interfaces/AppStore";
import { hashNumbersRemover, removeFromEnd, removeFromStart } from './PlanParserUtils';


export function specialSplit(input: string): string[] {
    const result: string[] = [];
    let buffer = "";
    let bracketCount = 0;
    let inQuotes = false;

    for (let i = 0; i < input.length; i++) {
        const char = input[i];

        if (char === "(") bracketCount++;
        if (char === ")") bracketCount--;
        if (char === '"') inQuotes = !inQuotes;

        if (char === "," && bracketCount === 0 && !inQuotes) {
            result.push(buffer.trim());
            buffer = "";
        } else {
            buffer += char;
        }
    }
    if (buffer) result.push(buffer.trim());
    return result;
}

export function parseJoin(input: string): ParsedJoinPlan {
    if (input.startsWith("BroadcastNestedLoopJoin")) {
        return { joinType: "BroadcastNestedLoopJoin", joinSideType: "Cross" };
    }

    const regex = /^(\w+)\s+\[(.*?)\], \[(.*?)\], (\w+)(?:,\s+(.*))?$/;
    const match = hashNumbersRemover(input).match(regex);

    if (!match) {
        throw new Error('Invalid input format');
    }

    const [, joinType, leftKeysStr, rightKeysStr, joinSideType, conditionStr] = match;
    let leftKeys: string[] | undefined;
    let rightKeys: string[] | undefined;
    let joinCondition: string | undefined;

    if (leftKeysStr) {
        leftKeys = specialSplit(leftKeysStr)
    }

    if (rightKeysStr) {
        rightKeys = specialSplit(rightKeysStr);
    }

    if (conditionStr) {
        joinCondition = conditionStr;
        joinCondition = removeFromEnd(joinCondition, ", false");
        joinCondition = removeFromStart(joinCondition, "BuildRight, ");
        joinCondition = removeFromStart(joinCondition, "BuildLeft, ");
    }

    if (joinCondition === "BuildRight") {
        joinCondition = undefined;
    }
    if (joinCondition === "BuildLeft") {
        joinCondition = undefined;
    }

    return {
        joinType,
        leftKeys,
        rightKeys,
        joinCondition,
        joinSideType
    };
}
