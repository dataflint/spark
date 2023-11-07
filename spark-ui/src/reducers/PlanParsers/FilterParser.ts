import { ParseFilterPlan } from "../../interfaces/AppStore";
import { hashNumbersRemover } from "./PlanParserUtils";

export function parseFilter(
  input: string
): ParseFilterPlan {
  const condition = hashNumbersRemover(input.replace("Filter (", "").slice(0, -1))
  return { condition: condition };
}
