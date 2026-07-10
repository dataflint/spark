import { ParsedClickHouseWritePlan } from "../../interfaces/AppStore";

// The ClickHouse connector's Write.description renders as:
//   ClickHouseWrite(database=<db>, table=<table>)})
export function parseWriteToClickHouse(input: string): ParsedClickHouseWritePlan {
  const databaseMatch = /ClickHouseWrite\(database=([^,]+),/.exec(input);
  const tableMatch = /table=([^)]+)\)/.exec(input);
  return {
    database: databaseMatch?.[1]?.trim() ?? "",
    tableName: tableMatch?.[1]?.trim() ?? "",
  };
}
