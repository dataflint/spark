export function onlyUnique(value: string, index: number, array: string[]) {
    return array.indexOf(value) === index;
  }

export function hashNumbersRemover(input: string): string {
    return input.replace(/#\d+/g, '')
}
