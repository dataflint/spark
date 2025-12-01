import { parseJDBCScan } from "./JDBCScanParser";

describe("JDBCScanParser", () => {
    describe("Simple table scan", () => {
        it("should parse simple table scan with single partition", () => {
            const nodeName = "Scan JDBCRelation(employees) [numPartitions=1]";
            const planDescription =
                "Scan JDBCRelation(employees) [numPartitions=1] [ID#0,NAME#1,DEPT_ID#2,SALARY#3,HIRE_DATE#4] PushedFilters: [], ReadSchema: struct<ID:int,NAME:string,DEPT_ID:int,SALARY:decimal(10,2),HIRE_DATE:date>";

            const result = parseJDBCScan(planDescription, nodeName);

            expect(result.numPartitions).toBe(1);
            expect(result.tableName).toBe("employees");
            expect(result.isFullScan).toBe(true);
            expect(result.isCustomQuery).toBe(false);
            expect(result.pushedFilters).toBeUndefined();
            expect(result.selectedColumns).toEqual(["ID", "NAME", "DEPT_ID", "SALARY", "HIRE_DATE"]);
            expect(result.readSchema).toEqual({
                ID: "int",
                NAME: "string",
                DEPT_ID: "int",
                SALARY: "decimal(10,2)",
                HIRE_DATE: "date",
            });
        });

        it("should parse table scan with multiple partitions", () => {
            const nodeName = "Scan JDBCRelation(employees) [numPartitions=4]";
            const planDescription =
                "Scan JDBCRelation(employees) [numPartitions=4] [ID#72,NAME#73,DEPT_ID#74,SALARY#75,HIRE_DATE#76] PushedFilters: [], ReadSchema: struct<ID:int,NAME:string,DEPT_ID:int,SALARY:decimal(10,2),HIRE_DATE:date>";

            const result = parseJDBCScan(planDescription, nodeName);

            expect(result.numPartitions).toBe(4);
            expect(result.tableName).toBe("employees");
            expect(result.isFullScan).toBe(true);
            expect(result.selectedColumns).toEqual(["ID", "NAME", "DEPT_ID", "SALARY", "HIRE_DATE"]);
        });
    });

    describe("Scan with pushed filters", () => {
        it("should parse scan with pushed down filters", () => {
            const nodeName = "Scan JDBCRelation(employees) [numPartitions=1]";
            const planDescription =
                "Scan JDBCRelation(employees) [numPartitions=1] [ID#41,NAME#42,DEPT_ID#43,SALARY#44,HIRE_DATE#45] PushedFilters: [*IsNotNull(SALARY), *GreaterThan(SALARY,75000.00)], ReadSchema: struct<ID:int,NAME:string,DEPT_ID:int,SALARY:decimal(10,2),HIRE_DATE:date>";

            const result = parseJDBCScan(planDescription, nodeName);

            expect(result.numPartitions).toBe(1);
            expect(result.tableName).toBe("employees");
            expect(result.isFullScan).toBe(false);
            expect(result.isCustomQuery).toBe(false);
            expect(result.selectedColumns).toEqual(["ID", "NAME", "DEPT_ID", "SALARY", "HIRE_DATE"]);
            expect(result.pushedFilters).toEqual([
                "*IsNotNull(SALARY)",
                "*GreaterThan(SALARY",
                "75000.00)",
            ]);
        });

        it("should parse scan with IsNotNull filter", () => {
            const nodeName = "Scan JDBCRelation(employees) [numPartitions=1]";
            const planDescription =
                "Scan JDBCRelation(employees) [numPartitions=1] [ID#0,DEPT_ID#2,SALARY#3] PushedFilters: [*IsNotNull(DEPT_ID)], ReadSchema: struct<ID:int,DEPT_ID:int,SALARY:decimal(10,2)>";

            const result = parseJDBCScan(planDescription, nodeName);

            expect(result.selectedColumns).toEqual(["ID", "DEPT_ID", "SALARY"]);
            expect(result.pushedFilters).toEqual(["*IsNotNull(DEPT_ID)"]);
            expect(result.isFullScan).toBe(false);
        });
    });

    describe("Custom SQL query scan", () => {
        it("should parse custom SQL query with JOIN", () => {
            const nodeName =
                "Scan JDBCRelation((SELECT e.id, e.name, e.salary, d.dept_name, d.location\n       FROM employees e\n       JOIN departments d ON e.dept_id = d.dept_id\n       WHERE e.salary > 60000\n       ORDER BY e.salary DESC) AS custom_query) [numPartitions=1]";
            const planDescription =
                "Scan JDBCRelation((SELECT e.id, e.name, e.salary, d.dept_name, d.location\n       FROM employees e\n       JOIN departments d ON e.dept_id = d.dept_id\n       WHERE e.salary > 60000\n       ORDER BY e.salary DESC) AS custom_query) [numPartitions=1] [ID#104,NAME#105,SALARY#106,DEPT_NAME#107,LOCATION#108] PushedFilters: [], ReadSchema: struct<ID:int,NAME:string,SALARY:decimal(10,2),DEPT_NAME:string,LOCATION:string>";

            const result = parseJDBCScan(planDescription, nodeName);

            expect(result.numPartitions).toBe(1);
            expect(result.isCustomQuery).toBe(true);
            expect(result.isFullScan).toBe(false);
            expect(result.selectedColumns).toEqual(["ID", "NAME", "SALARY", "DEPT_NAME", "LOCATION"]);
            expect(result.customQuery).toContain("SELECT e.id, e.name, e.salary");
            expect(result.customQuery).toContain("FROM employees e");
            expect(result.customQuery).toContain("JOIN departments d");
            expect(result.sql).toContain("SELECT e.id, e.name, e.salary");
            expect(result.sql).toContain("FROM employees e");
            expect(result.sql).toContain("JOIN departments d");
            expect(result.tableName).toBe("employees"); // Extracted from FROM clause
        });
    });

    describe("Scan with selected columns", () => {
        it("should parse departments table scan", () => {
            const nodeName = "Scan JDBCRelation(departments) [numPartitions=1]";
            const planDescription =
                "Scan JDBCRelation(departments) [numPartitions=1] [DEPT_ID#135,DEPT_NAME#136,LOCATION#137] PushedFilters: [], ReadSchema: struct<DEPT_ID:int,DEPT_NAME:string,LOCATION:string>";

            const result = parseJDBCScan(planDescription, nodeName);

            expect(result.numPartitions).toBe(1);
            expect(result.tableName).toBe("departments");
            expect(result.isFullScan).toBe(true);
            expect(result.selectedColumns).toEqual(["DEPT_ID", "DEPT_NAME", "LOCATION"]);
            expect(result.readSchema).toEqual({
                DEPT_ID: "int",
                DEPT_NAME: "string",
                LOCATION: "string",
            });
        });
    });

    describe("Edge cases", () => {
        it("should parse with numPartitions before column list", () => {
            const nodeName = "Scan JDBCRelation(employees) [numPartitions=1]";
            const planDescription =
                "Scan JDBCRelation(employees) [numPartitions=1] [id#519,name#520,dept_id#521,salary#522] PushedFilters: [], ReadSchema: struct<id:int,name:string,dept_id:int,salary:decimal(10,2)>";

            const result = parseJDBCScan(planDescription, nodeName);

            expect(result.numPartitions).toBe(1);
            expect(result.tableName).toBe("employees");
            expect(result.selectedColumns).toEqual(["id", "name", "dept_id", "salary"]);
        });

        it("should handle empty pushed filters", () => {
            const nodeName = "Scan JDBCRelation(test_table) [numPartitions=1]";
            const planDescription =
                "Scan JDBCRelation(test_table) [numPartitions=1] [] PushedFilters: [], ReadSchema: struct<>";

            const result = parseJDBCScan(planDescription, nodeName);

            expect(result.tableName).toBe("test_table");
            expect(result.pushedFilters).toBeUndefined();
        });

        it("should handle schema with complex types", () => {
            const nodeName = "Scan JDBCRelation(complex_table) [numPartitions=2]";
            const planDescription =
                "Scan JDBCRelation(complex_table) [numPartitions=2] [id#1,data#2] PushedFilters: [], ReadSchema: struct<id:bigint,data:string>";

            const result = parseJDBCScan(planDescription, nodeName);

            expect(result.numPartitions).toBe(2);
            expect(result.readSchema).toEqual({
                id: "bigint",
                data: "string",
            });
        });
    });
});

