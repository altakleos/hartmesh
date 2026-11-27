"use client";

import { selectAboutMarkdown, useBranding, useFeatures } from "@/core/features";
import { useProductName } from "@/core/i18n/context";
import { useI18n } from "@/core/i18n/hooks";
import {
  ProductLogo,
  ProviderIdentity,
  useProductIdentity,
} from "@/core/product/context";
import { SafeStreamdown } from "@/core/streamdown/components";
import { streamdownPluginsWithoutRawHtml } from "@/core/streamdown/plugins";
import { APP_VERSION } from "@/version";

import { aboutMarkdown } from "./about-content";

export function AboutSettingsPage() {
  const { companyName, isLoading, providerName, supportURL } = useBranding();
  const { data: about, isError } = useFeatures(selectAboutMarkdown);
  const { t } = useI18n();
  const productName = useProductName();
  const identity = useProductIdentity();
  if (about === undefined) {
    return (
      <p role="status" className="text-muted-foreground text-sm">
        {isError ? t.workspace.gatewayUnavailable : t.common.loading}
      </p>
    );
  }
  if (about !== null) {
    return about.trim() ? (
      <SafeStreamdown
        {...streamdownPluginsWithoutRawHtml}
        mode="static"
        controls={false}
        linkSafety={{ enabled: false }}
      >
        {about}
      </SafeStreamdown>
    ) : null;
  }
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
