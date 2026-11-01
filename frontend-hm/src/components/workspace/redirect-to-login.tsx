"use client";

import { useEffect } from "react";

import { currentReturnPath } from "@/core/auth/next-path";
import { buildLoginUrl } from "@/core/auth/types";
import { useI18n } from "@/core/i18n/hooks";

/**
 * A layout receives no query, so only the browser can say where a person was
 * going. A link from a bot opened signed out comes back to its page and query.
 */
export function RedirectToLogin() {
  const { t } = useI18n();

  useEffect(() => {
    window.location.replace(buildLoginUrl(currentReturnPath()));
  }, []);

  return (
    <p className="text-muted-foreground p-4 text-sm">{t.common.loading}</p>
  );
}
