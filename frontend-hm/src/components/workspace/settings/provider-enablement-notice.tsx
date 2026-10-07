"use client";

import { useBranding } from "@/core/features/hooks";
import { useI18n } from "@/core/i18n/hooks";

export function ProviderEnablementNotice() {
  const { t } = useI18n();
  const { providerName, supportURL } = useBranding();
  return (
    <p className="text-muted-foreground text-sm">
      {t.settings.providerEnablement}{" "}
      {supportURL && (
        <a
          href={supportURL}
          target="_blank"
          rel="noopener noreferrer"
          className="underline"
        >
          {providerName
            ? t.workspace.providerSupport(providerName)
            : t.workspace.contactSupport}
        </a>
      )}
    </p>
  );
}
