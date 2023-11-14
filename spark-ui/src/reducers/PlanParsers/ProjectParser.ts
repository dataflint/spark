import { ParsedProjectPlan } from "../../interfaces/AppStore";
import { hashNumbersRemover } from "./PlanParserUtils";

export function parseProject(
  input: string
): ParsedProjectPlan {
  const fieldsStr = hashNumbersRemover(input.replace("Project [", "").slice(0, -1))
  const fields = fieldsStr.split(",").map(field => field.trim());
  return { fields: fields };
}
