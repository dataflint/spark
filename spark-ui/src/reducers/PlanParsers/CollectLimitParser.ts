import { ParsedCollectLimitPlan } from "../../interfaces/AppStore";

export function parseCollectLimit(input: string): ParsedCollectLimitPlan {
    return {
        limit: parseInt(input.split(" ")[1])
    }
}

