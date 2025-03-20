import { ParsedCoalescePlan } from '../../interfaces/AppStore';
import { parseCoalesce } from './CoalesceParser';

describe('CoalesceParser', () => {
    it('should parse the partition number from a Coalesce plan', () => {
        const input = 'Coalesce 10';
        const expected: ParsedCoalescePlan = { partitionNum: 10 };
        const result = parseCoalesce(input);
        expect(result).toEqual(expected);
    });

    it('should handle invalid input gracefully', () => {
        const input = 'Coalesce';
        expect(() => parseCoalesce(input)).toThrowError();
    });
}); 