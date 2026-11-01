/** One pattern describes a bot token everywhere; the server keeps the same string. */
export const TOKEN_BODY = "[0-9]{6,12}:[A-Za-z0-9_-]{30,}";

const TOKEN = new RegExp(`(?<![0-9])${TOKEN_BODY}`);

/**
 * The first token in a pasted text, which may be the token alone, BotFather's
 * whole message or an address that holds it. Call this on a paste, on a press
 * and when the field loses the focus, never while typing: the pattern has no
 * upper bound, so a token typed by hand would match before it is complete.
 */
export function findToken(text: string): string | null {
  return TOKEN.exec(text)?.[0] ?? null;
}
