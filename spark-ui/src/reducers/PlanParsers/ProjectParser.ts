import { ParsedProjectPlan } from "../../interfaces/AppStore";
import { bracedSplit, hashNumbersRemover } from "./PlanParserUtils";

export function parseProject(input: string): ParsedProjectPlan {
  // If the input is just "Project", "PhotonProject", or "GpuProject", return empty fields
  if (input === "Project" || input === "PhotonProject" || input === "GpuProject") {
    return { fields: [] };
  }

  let fieldsStr = input;
  // Remove the project type and opening bracket in the correct order
  if (fieldsStr.startsWith("PhotonProject [")) {
    fieldsStr = fieldsStr.replace("PhotonProject [", "");
  } else if (fieldsStr.startsWith("GpuProject [")) {
    fieldsStr = fieldsStr.replace("GpuProject [", "");
  } else if (fieldsStr.startsWith("Project [")) {
    fieldsStr = fieldsStr.replace("Project [", "");
  }

  // Remove the closing bracket and hash numbers
  fieldsStr = hashNumbersRemover(fieldsStr.slice(0, -1));

  const fields = bracedSplit(fieldsStr).map((field) => field.trim()).filter((field) => field.length > 0);
  return { fields: fields };
}
