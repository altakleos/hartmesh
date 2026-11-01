/** White text has a contrast of at least 4.9 to 1 on each; the server keeps the same list. */
export const PICTURE_COLORS: readonly string[] = [
  "#1D4ED8",
  "#0F766E",
  "#15803D",
  "#A16207",
  "#C2410C",
  "#B91C1C",
  "#A21CAF",
  "#6D28D9",
];

export const CHAT_COLOR = "#334155";

function fnv1a(key: string): number {
  let hash = 0x811c9dc5;
  for (const byte of new TextEncoder().encode(key)) {
    hash = Math.imul(hash ^ byte, 0x01000193);
  }
  return hash >>> 0;
}

function firstCharacter(word: string): string {
  const [first = ""] = Array.from(word);
  // One character can have a longer upper case, as ß has; the server keeps the first.
  const [upper = ""] = Array.from(first.toUpperCase());
  return upper;
}

export function pictureOf(
  name: string,
  key: string,
): { initials: string; color: string } {
  const words = name.match(/[\p{L}\p{N}]+/gu) ?? [];
  return {
    initials: words.slice(0, 2).map(firstCharacter).join(""),
    color: PICTURE_COLORS[(fnv1a(key) >>> 16) % PICTURE_COLORS.length]!,
  };
}
