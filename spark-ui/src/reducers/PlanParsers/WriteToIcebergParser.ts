import { ParsedIcebergWritePlan } from "../../interfaces/AppStore";

export function parseWriteToIceberg(input: string): ParsedIcebergWritePlan {
  const tableMatch = /IcebergWrite\(table=([^,]+),/.exec(input);
  const formatMatch = /format=([^)]+)\)/.exec(input);
  return {
    tableName: tableMatch?.[1] ?? "",
    format: formatMatch?.[1] ?? "",
    tableType: "Iceberg",
  };
}