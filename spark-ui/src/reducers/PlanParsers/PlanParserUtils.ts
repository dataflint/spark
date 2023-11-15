export function onlyUnique(value: string, index: number, array: string[]) {
  return array.indexOf(value) === index;
}

export function hashNumbersRemover(input: string): string {
  return input.replace(/#\d+L/g, "").replace(/#\d+/g, "");
}

export function truncateMiddle(str: string, maxLength: number): string {
  if (str.length <= maxLength) {
    return str;
  }

  const prefixLength = Math.ceil(maxLength / 2) - 1; // Subtract 1 for the '...'
  const suffixLength = Math.floor(maxLength / 2);

  const prefix = str.substring(0, prefixLength);
  const suffix = str.substring(str.length - suffixLength);

  return `${prefix}...${suffix}`;
}

export function removeFromStart(str: string, strToRemove: string): string {
  if (str.startsWith(strToRemove)) {
    return str.slice(strToRemove.length);
  }
  return str;
}

export function removeFromEnd(str: string, strToRemove: string) {
  if (str.endsWith(strToRemove)) {
    return str.slice(0, -strToRemove.length);
  }
  return str;
}
