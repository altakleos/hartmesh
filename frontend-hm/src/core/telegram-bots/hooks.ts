import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useCallback } from "react";

import { useAuth } from "@/core/auth/AuthProvider";
import { useBranding, useTelegram } from "@/core/features/hooks";
import { useI18n } from "@/core/i18n/hooks";

import { listTelegramBots } from "./api";
import type { TelegramReader } from "./text";

/** Every account's list starts with this, so one refresh reaches them all. */
export const telegramBotsQueryPrefix = ["telegram-bots"] as const;

/**
 * The one list every place reads. It is read whenever the features answer is
 * known, also while Telegram cannot run, so a person still sees that the bots
 * are kept. The account is part of the key, so nothing crosses a sign-in.
 */
export function useTelegramBots({
  refetchInterval,
}: { refetchInterval?: number } = {}) {
  const { user } = useAuth();
  const { known } = useTelegram();
  return useQuery({
    queryKey: [...telegramBotsQueryPrefix, user?.id ?? null],
    queryFn: listTelegramBots,
    enabled: known,
    refetchInterval,
  });
}

export function useRefreshTelegramBots() {
  const queryClient = useQueryClient();
  return useCallback(
    () => queryClient.invalidateQueries({ queryKey: telegramBotsQueryPrefix }),
    [queryClient],
  );
}

/** Who reads a text that says whom to ask: no text sends a user to the provider. */
export function useTelegramReader(): TelegramReader {
  const { user } = useAuth();
  const { providerName } = useBranding();
  const { t } = useI18n();
  return {
    admin: user?.system_role === "admin",
    provider: providerName ?? t.agentInstances.providerFallback,
  };
}
