import type { Locale } from "./locale";
import { createEnUS } from "./locales/en-US";
import type { Translations } from "./locales/types";
import { createZhCN } from "./locales/zh-CN";

// Translation dictionaries contain formatter functions, so they must be
// selected inside a Client Component rather than serialized through an RSC
// boundary. Each is built for the deployment's product name.
export const clientTranslations: Record<
  Locale,
  (productName: string) => Translations
> = {
  "en-US": createEnUS,
  "zh-CN": createZhCN,
};
