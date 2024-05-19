import { ParsedProjectPlan } from "../../interfaces/AppStore";
import { bracedSplit, hashNumbersRemover } from "./PlanParserUtils";

export function parseProject(input: string): ParsedProjectPlan {
  const fieldsStr = hashNumbersRemover(
    input.replace("Project [", "").replace("PhotonProject [", "").slice(0, -1),
  );
  const fields = bracedSplit(fieldsStr).map((field) => field.trim());
  return { fields: fields };
}
