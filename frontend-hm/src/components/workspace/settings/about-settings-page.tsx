"use client";

import { useBranding } from "@/core/features";
import { SafeStreamdown } from "@/core/streamdown/components";

import { aboutMarkdown, brandedAboutMarkdown } from "./about-content";

export function AboutSettingsPage() {
  const { companyName, isLoading } = useBranding();
  if (isLoading) {
    return null;
  }
  return (
    <SafeStreamdown>
      {companyName === null ? aboutMarkdown : brandedAboutMarkdown(companyName)}
    </SafeStreamdown>
  );
}
