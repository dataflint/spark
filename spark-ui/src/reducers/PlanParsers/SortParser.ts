import { ParsedSortPlan } from "../../interfaces/AppStore";
import { hashNumbersRemover } from "./PlanParserUtils";

export function parseSort(input: string): ParsedSortPlan {
  const match = hashNumbersRemover(input).match(/\[(.*?)\]/);
  if (!match) {
    return { fields: [] };
  }
  const fields = match[1].split(",").map((field) => field.trim());
  return { fields: fields };
}
