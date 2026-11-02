"use client";

import { useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { toast } from "sonner";

import {
  closeConnectWindow,
  openConnectUrl,
  prepareConnectWindow,
} from "@/core/channels/open-connect-url";
import { useI18n } from "@/core/i18n/hooks";
import {
  type Bot,
  disconnectTelegramBot,
  requestTelegramConnect,
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

import {
  forgetTelegramReplaced,
  holdTelegramConnect,
  telegramConnectEpoch,
} from "./setup-dialog-store";

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

/**
 * One request at a time: a second link ends the first, along with its tab.
 * A bot that connected meanwhile is no failure; the list shows it.
 */
export function useTelegramConnect(
  bot: Bot | null,
  onFailure: (error: unknown) => void,
) {
  const refresh = useRefreshTelegramBots();
  const [busy, setBusy] = useState(false);

  const request = async ({ open }: { open: boolean }) => {
    if (!bot || busy) {
      return;
    }
    setBusy(true);
    const tab = open ? prepareConnectWindow() : null;
    const epoch = telegramConnectEpoch();
    try {
      const connect = await requestTelegramConnect(bot.id);
      if (!holdTelegramConnect(bot.id, connect, epoch)) {
        closeConnectWindow(tab);
      } else if (open) {
        openConnectUrl(connect.url, tab);
      }
    } catch (error) {
      closeConnectWindow(tab);
      if (
        !(error instanceof TelegramRefusal) ||
        error.reason !== "already-connected"
      ) {
        onFailure(error);
      }
    }
    await refresh();
    setBusy(false);
  };

  return { busy, request };
}

/** Disconnected from another tab already is the same answer, not a failure. */
export function useTelegramDisconnect() {
  const refresh = useRefreshTelegramBots();
  const failure = useTelegramFailure();
  const [busy, setBusy] = useState(false);

  const disconnect = async (bot: Bot, name: string, onDone?: () => void) => {
    if (busy) {
      return;
    }
    setBusy(true);
    const epoch = telegramConnectEpoch();
    try {
      const answer = await disconnectTelegramBot(bot.id);
      forgetTelegramReplaced(bot.id);
      holdTelegramConnect(bot.id, answer.connect, epoch);
      onDone?.();
    } catch (error) {
      if (
        !(error instanceof TelegramRefusal) ||
        error.reason !== "not-connected"
      ) {
        failure.show(error, name);
      }
    }
    await refresh();
    setBusy(false);
  };

  return { busy, disconnect };
}
