"use client";

import { useBranding } from "@/core/features";
import { useProductName } from "@/core/i18n/context";
import { SafeStreamdown } from "@/core/streamdown/components";

import { aboutMarkdown } from "./about-content";

export function AboutSettingsPage() {
  const { companyName, isLoading } = useBranding();
  const productName = useProductName();
  if (isLoading) {
    return null;
  }
  return (
    <SafeStreamdown>{aboutMarkdown(companyName ?? productName)}</SafeStreamdown>
  );
}
