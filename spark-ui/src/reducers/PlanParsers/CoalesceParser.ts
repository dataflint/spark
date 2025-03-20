import { ParsedCoalescePlan } from "../../interfaces/AppStore";

export function parseCoalesce(input: string): ParsedCoalescePlan {
    return {
        partitionNum: parseInt(input.split(" ")[1]),
    };
} 