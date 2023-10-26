import { ParsedTakeOrderedAndProjectPlan } from "../../interfaces/AppStore";
import { hashNumbersRemover } from "./PlanParserUtils";

export function parseTakeOrderedAndProject(
  input: string,
): ParsedTakeOrderedAndProjectPlan {
  const cleanInput = hashNumbersRemover(input);
  const outputMatch = cleanInput.match(/output=\[([^\]]+)\]/);
  const orderByMatch = cleanInput.match(/orderBy=\[([^\]]+)\]/);
  const limitMatch = cleanInput.match(/limit=(\d+)/);

  return {
    output: outputMatch ? outputMatch[1].split(",") : [],
    orderBy: orderByMatch ? orderByMatch[1].split(",") : [],
    limit: limitMatch ? parseInt(limitMatch[1], 10) : 0,
  };
}
