import type { Locale } from "./locale";
import type { Translations } from "./locales";

const translationLoaders: Record<
  Locale,
  () => Promise<(productName: string) => Translations>
> = {
  "en-US": async () => (await import("./locales/en-US")).createEnUS,
  "zh-CN": async () => (await import("./locales/zh-CN")).createZhCN,
};

export async function loadTranslations(
  locale: Locale,
  productName: string,
): Promise<Translations> {
  return (await translationLoaders[locale]())(productName);
}
