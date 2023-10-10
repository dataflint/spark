import { ParsedTakeOrderedAndProjectPlan } from "../../interfaces/AppStore";
import { parseTakeOrderedAndProject } from "./TakeOrderedAndProjectParser";

// Parametrized Unit Tests using Jest
describe('parseTakeOrderedAndProject', () => {
    const testCases: {input: string, expected: ParsedTakeOrderedAndProjectPlan}[] = [
        {
            input: 'TakeOrderedAndProject(limit=100, orderBy=[s_store_name#1001 ASC NULLS FIRST], output=[s_store_name#1001,sum(ss_net_profit)#26850])',
            expected: {
                output: ['s_store_name', 'sum(ss_net_profit)'],
                orderBy: ['s_store_name ASC NULLS FIRST'],
                limit: 100
            }
        },
        // ... add other test cases here
    ];

    testCases.forEach(({input, expected}) => {
        it(`should parse "${input}" correctly`, () => {
            expect(parseTakeOrderedAndProject(input)).toEqual(expected);
        });
    });
});
