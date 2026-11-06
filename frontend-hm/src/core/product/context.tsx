"use client";

import {
  createContext,
  useContext,
  useEffect,
  useState,
  type ReactNode,
} from "react";

import type { Locale } from "@/core/i18n";
import { I18nProvider } from "@/core/i18n/context";

import { IDENTITY_UNAVAILABLE, type ProductIdentity } from "./identity";

const ProductContext = createContext<ProductIdentity | null>(null);
export const useProductIdentity = () => useContext(ProductContext);

/** One complete identity per mounted document. A failed navigation retains it. */
export function RuntimeProductProvider({
  snapshot,
  initialLocale,
  children,
}: {
  snapshot: ProductIdentity | null;
  initialLocale: Locale;
  children: ReactNode;
}) {
  const [identity] = useState(snapshot);
  useEffect(() => {
    const existing = Array.from(
      document.querySelectorAll<HTMLLinkElement>('link[rel="icon"]'),
    );
    const icon = document.createElement("link");
    icon.rel = "icon";
    icon.href =
      identity?.product.browser_icon ?? (identity ? "/favicon.ico" : "data:,");
    existing.forEach((node) => node.remove());
    document.head.append(icon);
    if (!identity) document.title = IDENTITY_UNAVAILABLE;
    return () => {
      icon.remove();
      existing.forEach((node) => document.head.append(node));
    };
  }, [identity]);
  if (!identity)
    return (
      <main className="flex min-h-screen flex-col items-center justify-center gap-4 bg-white text-black">
        <h1>{IDENTITY_UNAVAILABLE}</h1>
        <p>Reload to retry loading the service identity.</p>
        <button
          className="rounded border border-black px-4 py-2"
          onClick={() => window.location.reload()}
        >
          Reload
        </button>
      </main>
    );
  return (
    <ProductContext.Provider value={identity}>
      <I18nProvider initialLocale={initialLocale} productName={identity.name}>
        {snapshot === null && (
          <p role="status" className="bg-white p-2 text-black">
            Service identity reader unavailable. Retaining the verified identity
            for this page.
          </p>
        )}
        {children}
      </I18nProvider>
    </ProductContext.Provider>
  );
}

/** Generic provider placement: entry/setup, sidebar and product information. */
export function ProviderIdentity() {
  const identity = useProductIdentity();
  if (identity?.mode !== "configured") return null;
  const provider = identity.provider;
  return (
    <a
      href={provider.support_url}
      target="_blank"
      rel="noopener noreferrer"
      referrerPolicy="no-referrer"
      aria-label={`${provider.name} support`}
      className="flex max-w-full items-center gap-2 rounded bg-white p-2 text-sm text-black underline group-data-[collapsible=icon]:justify-center group-data-[collapsible=icon]:p-1"
    >
      <img
        src={provider.logo}
        alt=""
        width={24}
        height={24}
        className="size-6 object-contain group-data-[collapsible=icon]:size-5"
      />
      <span className="min-w-0 wrap-anywhere group-data-[collapsible=icon]:hidden">
        Service and support: <bdi>{provider.name}</bdi>
      </span>
    </a>
  );
}

export function ProductLogo() {
  const identity = useProductIdentity();
  return identity?.mode === "configured" ? (
    <img
      src={identity.product.logo}
      alt={`${identity.name} logo`}
      width={64}
      height={64}
      className="size-16 rounded bg-white object-contain p-1"
    />
  ) : null;
}
