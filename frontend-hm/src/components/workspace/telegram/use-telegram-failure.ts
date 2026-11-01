"use client";

import { useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";

import { useI18n } from "@/core/i18n/hooks";
import {
  TELEGRAM_SECTION_REFUSALS,
  TelegramRefusal,
  telegramRefusalKey,
} from "@/core/telegram-bots/api";
import {
  useRefreshTelegramBots,
  useTelegramBots,
  useTelegramReader,
} from "@/core/telegram-bots/hooks";
import {
  type TelegramStrings,
  telegramText,
  type TelegramValues,
} from "@/core/telegram-bots/text";

/** What a text about this bot can print: the reader, its name and the limit. */
export function useTelegramValues(name: string): TelegramValues {
  const reader = useTelegramReader();
  const { data } = useTelegramBots();
  return { ...reader, name, count: data?.limits.used ?? 0 };
}

/** The server has no name for a target that was deleted, only for its kind. */
export function refusalOther(
  strings: TelegramStrings,
  error: TelegramRefusal,
): string {
  return error.agent_name ?? strings.kind.custom_agent;
}

/** What a row action does with a refusal that its own section does not name. */
export function useTelegramFailure() {
  const { t } = useI18n();
  const reader = useTelegramReader();
  const queryClient = useQueryClient();
  const refresh = useRefreshTelegramBots();
  const { data } = useTelegramBots();

  /** True for the refusals that need no words of the caller's. */
  const handled = (error: unknown): boolean => {
    if (!(error instanceof TelegramRefusal) || error.reason === null) {
      return false;
    }
    if (TELEGRAM_SECTION_REFUSALS.has(error.reason)) {
      // The notice appears once the features answer is read again.
      void queryClient.invalidateQueries({ queryKey: ["features"] });
      return true;
    }
    if (error.reason === "not-found") {
      toast.error(t.telegramBots.refusal.gone);
      void refresh();
      return true;
    }
    return false;
  };

  const show = (error: unknown, name: string) => {
    if (handled(error)) {
      return;
    }
    const key =
      error instanceof TelegramRefusal
        ? telegramRefusalKey(error)
        : "refusal.failed";
    toast.error(
      telegramText(t.telegramBots, key, {
        ...reader,
        name,
        count: data?.limits.used ?? 0,
        other:
          error instanceof TelegramRefusal
            ? refusalOther(t.telegramBots, error)
            : "",
      }),
    );
  };

  return { handled, show };
}
