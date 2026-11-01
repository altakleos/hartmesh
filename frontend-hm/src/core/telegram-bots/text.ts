import type { Translations } from "@/core/i18n/locales/types";

export type TelegramStrings = Translations["telegramBots"];

type KeyOf<T> = {
  [K in keyof T & string]: T[K] extends string | ((values: never) => string)
    ? K
    : `${K}.${KeyOf<T[K]>}`;
}[keyof T & string];

/** A dotted key of the group, as the design and `rows.ts` write it. */
export type TelegramKey = KeyOf<TelegramStrings>;

/** Who reads a text; `rowOf` and every text that says whom to ask need it. */
export interface TelegramReader {
  admin: boolean;
  provider: string;
}

export interface TelegramValues {
  name?: string;
  telegramName?: string;
  time?: string;
  username?: string;
  other?: string;
  count?: number;
  admin?: boolean;
  provider?: string;
}

type TextEntry = string | ((values: TelegramValues) => string);

/** The text of a key; a text with a placeholder reads it from `values`. */
export function telegramText(
  strings: TelegramStrings,
  key: TelegramKey,
  values: TelegramValues = {},
): string {
  let entry: unknown = strings;
  for (const part of key.split(".")) {
    entry = (entry as Record<string, unknown>)[part];
  }
  const text = entry as TextEntry;
  return typeof text === "function" ? text(values) : text;
}
