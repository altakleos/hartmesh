"use client";

import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";

import type { Locale } from "@/core/i18n";
import type { Translations } from "@/core/i18n/locales";
import { DEFAULT_PRODUCT_NAME } from "@/core/product";

import { clientTranslations } from "./client-translations";

export interface I18nContextType {
  locale: Locale;
  setLocale: (locale: Locale) => void;
  t: Translations;
}

export const I18nContext = createContext<I18nContextType | null>(null);

export function I18nProvider({
  children,
  initialLocale,
  productName = DEFAULT_PRODUCT_NAME,
}: {
  children: ReactNode;
  initialLocale: Locale;
  /** Read by the server layout from `GET /api/product`; a plain string, so it crosses the RSC boundary. */
  productName?: string;
}) {
  const [locale, setLocale] = useState<Locale>(initialLocale);
  const t = useMemo(
    () => clientTranslations[locale](productName),
    [locale, productName],
  );

  useEffect(() => {
    document.documentElement.lang = locale;
  }, [locale]);

  return (
    <I18nContext.Provider value={{ locale, setLocale, t }}>
      {children}
    </I18nContext.Provider>
  );
}

export function useI18nContext() {
  const context = useContext(I18nContext);
  if (!context) {
    throw new Error("useI18n must be used within I18nProvider");
  }
  return context;
}

/**
 * What the deployment calls the product, as the layout read it for this page.
 * Read from the dictionary, which was built with it, so there is one copy of
 * the name and the words around it can never disagree with it.
 */
export function useProductName(): string {
  return useI18nContext().t.pages.appName;
}
