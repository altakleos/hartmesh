"use client";

import { useBranding } from "@/core/features";
import { useProductName } from "@/core/i18n/context";
import { useI18n } from "@/core/i18n/hooks";
import {
  ProductLogo,
  ProviderIdentity,
  useProductIdentity,
} from "@/core/product/context";
import { SafeStreamdown } from "@/core/streamdown/components";
import { APP_VERSION } from "@/version";

import { aboutMarkdown } from "./about-content";

export function AboutSettingsPage() {
  const { companyName, isLoading, providerName, supportURL } = useBranding();
  const { t } = useI18n();
  const productName = useProductName();
  const identity = useProductIdentity();
  if (identity?.mode === "configured") {
    return (
      <section className="space-y-4">
        <ProductLogo />
        <h1>{identity?.name ?? productName}</h1>
        <ProviderIdentity />
        <p>HartMesh component version {APP_VERSION}.</p>
      </section>
    );
  }
  if (isLoading) {
    return null;
  }
  return (
    <>
      <SafeStreamdown>
        {aboutMarkdown(companyName ?? productName)}
      </SafeStreamdown>
      {supportURL && (
        <a
          href={supportURL}
          target="_blank"
          rel="noopener noreferrer"
          referrerPolicy="no-referrer"
          className="text-sm underline"
        >
          {providerName
            ? t.workspace.providerSupport(providerName)
            : t.workspace.contactSupport}
        </a>
      )}
    </>
  );
}
