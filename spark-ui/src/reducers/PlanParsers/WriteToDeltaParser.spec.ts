import { parseWriteToDelta } from "./WriteToDeltaParser";

describe("parseWriteToDelta", () => {
    test("parses WriteIntoDeltaCommand correctly", () => {
        const input =
            "Execute WriteIntoDeltaCommand OutputSpec(s3://earnin-datalakeprod-us-west-2-databricks-uc-catalog-main/__unitystorage/catalogs/369ad184-0d16-469c-bcf9-b008e7d2bd1f/tables/fc2b1e27-6821-4485-b7ce-9e798fb848c7,Map(),Vector(context_device_id#2348, screen_type#2349, first_successful_login#2350, login_attempt_time#2351, login_result#2352, phone_verify_time#2353, email_verify_time#2354))";

        const result = parseWriteToDelta(input);

        expect(result).toEqual({
            location:
                "s3://earnin-datalakeprod-us-west-2-databricks-uc-catalog-main/__unitystorage/catalogs/369ad184-0d16-469c-bcf9-b008e7d2bd1f/tables/fc2b1e27-6821-4485-b7ce-9e798fb848c7",
            fields: [
                "context_device_id",
                "screen_type",
                "first_successful_login",
                "login_attempt_time",
                "login_result",
                "phone_verify_time",
                "email_verify_time",
            ],
        });
    });

    test("parses WriteIntoDeltaCommand with different fields", () => {
        const input =
            "Execute WriteIntoDeltaCommand OutputSpec(s3://bucket/path/to/table,Map(),Vector(id#100, name#101, age#102))";

        const result = parseWriteToDelta(input);

        expect(result).toEqual({
            location: "s3://bucket/path/to/table",
            fields: ["id", "name", "age"],
        });
    });
});
