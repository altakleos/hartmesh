"use client";

import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { useEffect, useState } from "react";

import { Button } from "@/components/ui/button";
import { ProviderSupportLink } from "@/components/workspace/instances/execution-notice";
import { useAgents } from "@/core/agents/hooks";
import { useBranding, useTelegram } from "@/core/features/hooks";
import { useI18n } from "@/core/i18n/hooks";
import type { Bot } from "@/core/telegram-bots/api";
import {
  useTelegramBots,
  useTelegramClock,
  useTelegramReader,
} from "@/core/telegram-bots/hooks";
import {
  rowOf,
  sectionNoticeOf,
  targetRowsOf,
} from "@/core/telegram-bots/rows";
import { telegramText } from "@/core/telegram-bots/text";
import { telegramTargetOf } from "@/core/telegram-bots/web-pages";

import { TelegramAdminBlock } from "./admin-block";
import { TelegramBotRow } from "./bot-row";
import { TelegramRowDialog, type TelegramRowDialogState } from "./row-dialogs";
import { TelegramSettingsBlock } from "./settings-block";
import { openTelegramSetup, useTelegramPageState } from "./setup-dialog-store";
import { TelegramTokenDialog } from "./token-dialog";

const LIST_INTERVAL_MS = 10_000;

/** One row per target, the notice for the whole section, and the blocks under them. */
export function TelegramBotsSection() {
  const { t } = useI18n();
  const strings = t.telegramBots;
  const feature = useTelegram();
  const reader = useTelegramReader();
  const { providerName, supportURL } = useBranding();
  const { agents, isLoading: agentsLoading } = useAgents();
  const list = useTelegramBots({ refetchInterval: LIST_INTERVAL_MS });
  const now = useTelegramClock();
  const { replaced } = useTelegramPageState();
  const [dialog, setDialog] = useState<TelegramRowDialogState | null>(null);
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const data = list.data;
  const bots = data?.bots ?? [];
  const limits = data?.limits ?? { per_person: 0, used: 0 };
  const notice = sectionNoticeOf(feature, bots.length > 0);
  const atLimit = data !== undefined && limits.used >= limits.per_person;
  const values = { ...reader, count: limits.used };
  const blocked = notice
    ? telegramText(strings, notice.key, values)
    : atLimit
      ? telegramText(strings, "unavailable.limitPerson", values)
      : null;
  const rows = targetRowsOf({
    chatName: strings.chatName,
    agentNames: agents.map((agent) => agent.name),
    bots,
  });
  const wanted = searchParams.get("telegram");
  // A late answer closes its own dialog, never one opened since.
  const closeDialog = (mine: TelegramRowDialogState) =>
    setDialog((open) => (open === mine ? null : open));

  // An address that names a row: the row takes the focus, or its set-up opens.
  useEffect(() => {
    if (wanted === null || data === undefined || agentsLoading) {
      return;
    }
    const row = rows.find(({ target }) => telegramTargetOf(target) === wanted);
    if (row?.bot) {
      const element = document.querySelector<HTMLElement>(
        `[data-telegram-target="${wanted}"]`,
      );
      element?.scrollIntoView();
      element?.focus();
    } else if (row && blocked === null) {
      openTelegramSetup(row.target);
    }
    const next = new URLSearchParams(searchParams);
    next.delete("telegram");
    const suffix = next.toString();
    router.replace(suffix ? `${pathname}?${suffix}` : pathname, {
      scroll: false,
    });
    // Runs when the address or the first answer arrives, and only then.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [wanted, data === undefined, agentsLoading]);

  if (!feature.known) {
    return null;
  }

  return (
    <section className="space-y-6">
      {notice ? (
        <div
          className="rounded-lg border p-3 text-sm"
          data-testid="telegram-notice"
          role="status"
        >
          <p>{telegramText(strings, notice.key, values)}</p>
          {notice.ending && !reader.admin ? (
            <p>{strings.unavailable.askAdmin}</p>
          ) : null}
          {notice.ending && reader.admin ? (
            <>
              {notice.next ? (
                <p>{strings.unavailable.next({ provider: reader.provider })}</p>
              ) : null}
              <ProviderSupportLink
                provider={providerName}
                supportURL={supportURL}
              />
              {feature.reasonCode ? (
                <p className="text-muted-foreground text-xs">
                  {strings.unavailable.reference({ code: feature.reasonCode })}
                </p>
              ) : null}
            </>
          ) : null}
        </div>
      ) : null}
      <header className="space-y-2">
        <h2 className="text-lg font-semibold">{strings.list.title}</h2>
        <p className="text-muted-foreground text-sm">{strings.list.intro}</p>
      </header>
      {data === undefined && !list.isError ? (
        <p className="text-muted-foreground text-sm">{t.common.loading}</p>
      ) : data === undefined ? (
        <div className="space-y-2">
          <p className="text-sm">{strings.unavailable.loadError}</p>
          <Button
            type="button"
            size="sm"
            variant="outline"
            onClick={() => void list.refetch()}
          >
            {strings.action.tryAgain}
          </Button>
        </div>
      ) : (
        <ul className="space-y-3">
          {rows.map((row) => (
            <TelegramBotRow
              key={telegramTargetOf(row.target)}
              row={row}
              disabledBecause={blocked}
              now={now}
              onDialog={setDialog}
              view={rowOf({
                name: row.name,
                bot: row.bot,
                feature,
                reader,
                now,
                replaced: row.bot ? replaced.has(row.bot.id) : false,
              })}
            />
          ))}
        </ul>
      )}
      {data && limits.used >= limits.per_person - 2 ? (
        <p className="text-muted-foreground text-sm">
          {strings.list.count({
            used: limits.used,
            limit: limits.per_person,
          })}
        </p>
      ) : null}
      {atLimit ? (
        <p className="text-sm">
          {strings.unavailable.limitPerson({ count: limits.used })}
        </p>
      ) : null}
      {bots.length > 0 ? (
        <section className="space-y-2">
          <h3 className="font-medium">{strings.lost.title}</h3>
          <p className="text-muted-foreground text-sm">{strings.lost.body}</p>
          <Button
            type="button"
            size="sm"
            variant="outline"
            onClick={() => setDialog({ kind: "lost", count: bots.length })}
          >
            {strings.lost.button}
          </Button>
        </section>
      ) : null}
      {data?.settings ? (
        <TelegramSettingsBlock settings={data.settings} />
      ) : null}
      {reader.admin ? <TelegramAdminBlock /> : null}
      {dialog?.kind === "token" ? (
        <TelegramTokenDialog
          bot={dialog.bot}
          name={dialog.name}
          onClose={() => closeDialog(dialog)}
        />
      ) : dialog ? (
        <TelegramRowDialog
          dialog={dialog}
          onClose={() => closeDialog(dialog)}
          onPasteToken={(bot: Bot, name: string) =>
            setDialog({ kind: "token", bot, name })
          }
        />
      ) : null}
    </section>
  );
}
