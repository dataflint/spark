import { ParseFilterPlan } from "../../interfaces/AppStore";
import { hashNumbersRemover, removeFromEnd, removeFromStart } from "./PlanParserUtils";

export function parseFilter(
  input: string
): ParseFilterPlan {
  let filterStr = input;
  filterStr = removeFromStart(filterStr, "Filter ");
  filterStr = removeFromStart(filterStr, "(");
  filterStr = removeFromEnd(filterStr, ")");
  const condition = hashNumbersRemover(filterStr)
  return { condition: condition };
}
