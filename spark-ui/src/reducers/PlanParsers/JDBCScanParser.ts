import { ParsedJDBCScanPlan } from "../../interfaces/AppStore";
import { hashNumbersRemover } from "./PlanParserUtils";

/**
 * Parses JDBC scan information from Spark plan description
 * 
 * Expected patterns:
 * - Scan JDBCRelation(table_name) [numPartitions=N]
 * - Scan JDBCRelation((SELECT ... FROM ...) AS custom_query) [numPartitions=N]
 * 
 * Plan description contains:
 * - Column list: [col1, col2, col3]
 * - PushedFilters: [filter1, filter2]
 * - ReadSchema: struct<...>
 */
export function parseJDBCScan(
    planDescription: string,
    nodeName: string,
): ParsedJDBCScanPlan {
    const cleanedInput = hashNumbersRemover(planDescription);
    const result: ParsedJDBCScanPlan = {};

    // Extract number of partitions from node name
    // Format: "Scan JDBCRelation(table_or_query) [numPartitions=N]"
    const numPartitionsMatch = /\[numPartitions=(\d+)\]/.exec(nodeName);
    if (numPartitionsMatch) {
        result.numPartitions = parseInt(numPartitionsMatch[1], 10);
    }

    // Extract table name or custom query from node name
    // Check if it's a custom SQL query (contains parentheses with SELECT)
    // Need to handle multiline SQL queries, so use [\s\S] to match any character including newlines
    const customQueryMatch = /JDBCRelation\(\(([\s\S]+)\) AS \w+\)/.exec(nodeName);
    const simpleTableMatch = /JDBCRelation\((\w+)\)/.exec(nodeName);

    if (customQueryMatch) {
        // It's a custom SQL query
        result.isCustomQuery = true;
        result.customQuery = customQueryMatch[1].trim();
        // Also store as 'sql' field for UI display
        result.sql = customQueryMatch[1].trim();

        // Try to extract table name from the custom query if it's simple
        const tableFromQueryMatch = /FROM\s+(\w+)/i.exec(result.customQuery);
        if (tableFromQueryMatch) {
            result.tableName = tableFromQueryMatch[1];
        }
    } else if (simpleTableMatch) {
        // It's a simple table scan
        result.isCustomQuery = false;
        result.tableName = simpleTableMatch[1];
    }

    // Extract pushed filters from plan description
    const pushedFiltersMatch = /PushedFilters: \[(.*?)\]/.exec(cleanedInput);
    if (pushedFiltersMatch) {
        const filtersStr = pushedFiltersMatch[1].trim();
        if (filtersStr && !filtersStr.includes("...")) {
            result.pushedFilters = filtersStr
                .split(",")
                .map((filter) => filter.trim())
                .filter((filter) => filter.length > 0 && filter !== "*");
        }
    }

    // Extract selected columns from plan description
    // Format: "Scan JDBCRelation(...) [numPartitions=1] [ID#0,NAME#1,DEPT_ID#2] PushedFilters: ..."
    // We need to skip [numPartitions=X] and get the column list
    // IMPORTANT: Use original planDescription, NOT cleanedInput, because hashNumbersRemover removes the # we need

    // Find all bracket contents from the ORIGINAL planDescription
    const allBracketsOriginal = planDescription.match(/\[([^\]]+)\]/g);
    if (allBracketsOriginal && allBracketsOriginal.length >= 2) {
        // The first bracket after JDBCRelation is usually [numPartitions=X]
        // The second bracket is the column list [ID#519,NAME#520,...]
        const secondBracket = allBracketsOriginal[1];
        const columnsStr = secondBracket.replace(/[\[\]]/g, '').trim();

        // Check if this looks like a column list (contains # character)
        if (columnsStr && columnsStr.includes('#') && !columnsStr.includes("...")) {
            result.selectedColumns = columnsStr
                .split(",")
                .map((col) => {
                    // Remove hash IDs like #519, #520, etc.
                    const columnName = col.trim().replace(/#\d+/g, "");
                    return columnName;
                })
                .filter((col) => col.length > 0);
        }
    }

    // Extract ReadSchema from plan description
    const readSchemaMatch = /ReadSchema: struct<([\w\W]+?)>/.exec(cleanedInput);
    if (readSchemaMatch) {
        const schemaStr = readSchemaMatch[1];
        if (!schemaStr.includes("...")) {
            try {
                // Parse schema fields: "field1:type1,field2:type2,..."
                const fields = schemaStr.split(/,(?![^()]*\))/);
                const schema: { [key: string]: string } = {};
                fields.forEach((field) => {
                    const [name, type] = field.split(":");
                    if (name !== undefined && type !== undefined) {
                        schema[name.trim()] = type.trim();
                    }
                });
                result.readSchema = schema;
            } catch (e) {
                console.log("Failed to parse JDBC ReadSchema", e);
            }
        }
    }

    // Determine if it's a full table scan or filtered scan
    if (result.pushedFilters && result.pushedFilters.length > 0) {
        result.isFullScan = false;
    } else if (result.isCustomQuery) {
        result.isFullScan = false; // Custom queries are considered non-full scans
    } else {
        result.isFullScan = true;
    }

    return result;
}

