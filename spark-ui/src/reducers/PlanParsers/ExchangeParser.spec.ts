import { parseExchange } from "./ExchangeParser";

describe('parseExchange', () => {
    test('parses hash partitioning correctly', () => {
        const input = 'Exchange hashpartitioning(ss_quantity#9, 200), REPARTITION_BY_COL, [plan_id=40]';
        expect(parseExchange(input)).toEqual({
            type: 'hashpartitioning',
            fields: ['ss_quantity'],
        });
    });

    test('parses single partition correctly', () => {
        const input = 'Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=14514]';
        expect(parseExchange(input)).toEqual({
            type: 'SinglePartition',
            fields: [],
        });
    });

    test('parses range partitioning correctly', () => {
        const input = 'Exchange rangepartitioning(ca_county#787 ASC NULLS FIRST, 200), ENSURE_REQUIREMENTS, [plan_id=83408]';
        expect(parseExchange(input)).toEqual({
            type: 'rangepartitioning',
            fields: ['ca_county ASC NULLS FIRST'],
        });
    });

    // Add more test cases as necessary
});
