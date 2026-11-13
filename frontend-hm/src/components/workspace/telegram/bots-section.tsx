"use client";

import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { Fragment, useEffect, useState } from "react";

import { Button } from "@/components/ui/button";
import { ProviderSupportLink } from "@/components/workspace/instances/execution-notice";
import { hasCeiling } from "@/core/agent-instances/presentation";
import { useAgents } from "@/core/agents/hooks";
import { useAuth } from "@/core/auth/AuthProvider";
import { useBranding, useTelegram } from "@/core/features/hooks";
import { useI18n } from "@/core/i18n/hooks";
import type { Bot } from "@/core/telegram-bots/api";
import {
  useTelegramBots,
  useTelegramClock,
  useTelegramReader,
} from "@/core/telegram-bots/hooks";
import {
  useTelegramInstanceDiscovery,
  useTelegramInstanceDetail,
} from "@/core/telegram-bots/instance-hooks";
import { instanceConnectBlock } from "@/core/telegram-bots/instance-state";
import {
  MESSAGE_SETTINGS_SHOWN,
  rowOf,
  sectionNoticeOf,
  targetRowsOf,
} from "@/core/telegram-bots/rows";
import { telegramText } from "@/core/telegram-bots/text";
import { telegramTargetOf } from "@/core/telegram-bots/web-pages";

import { TelegramAdminBlock } from "./admin-block";
import { TelegramBotRow, TelegramExecutionNotice } from "./bot-row";
import { TelegramInstanceSettingsRow } from "./instance-settings-row";
import { TelegramRemovedNotices } from "./removed-notices";
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
  const { user } = useAuth();
  const discovery = useTelegramInstanceDiscovery();
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
  const missingList = data === undefined;
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
    instances: discovery.instances,
    ownerId: user?.id,
  });
  const wanted = searchParams.get("telegram");
  const wantedRow = rows.find(
    ({ target }) => telegramTargetOf(target) === wanted,
  );
  const wantedDetail = useTelegramInstanceDetail(
    wantedRow?.bot ? undefined : wantedRow?.instance,
  );
  const firstInstance = rows.find((row) => row.target.kind === "agent");
  // A late answer closes its own dialog, never one opened since.
  const closeDialog = (mine: TelegramRowDialogState) =>
    setDialog((open) => (open === mine ? null : open));

  // An address that names a row: the row takes the focus, or its set-up opens.
  useEffect(() => {
    if (wanted === null || data === undefined || agentsLoading) {
      return;
    }
    const row = wantedRow;
    if (wanted.startsWith("agent.") && !row?.bot) {
      if (
        !discovery.complete ||
        (row?.instance && (wantedDetail.isPending || wantedDetail.isError))
      )
        return;
    }
    if (row?.bot) {
      const element = document.querySelector<HTMLElement>(
        `[data-telegram-target="${wanted}"]`,
      );
      element?.scrollIntoView();
      element?.focus();
    } else if (row && blocked === null) {
      if (row.target.kind !== "agent") openTelegramSetup(row.target);
      else if (
        wantedDetail.data &&
        !instanceConnectBlock({
          instance: wantedDetail.data,
          telegram: feature,
          feature: discovery.feature,
          bots: data,
          canRead: !!user && hasCeiling(user.permissions, "agents:read"),
        })
      ) {
        openTelegramSetup({
          kind: "agent",
          id: wantedDetail.data.id,
          name: wantedDetail.data.name,
          permissions: wantedDetail.data.permissions,
        });
      }
    }
    const next = new URLSearchParams(searchParams);
    next.delete("telegram");
    const suffix = next.toString();
    router.replace(suffix ? `${pathname}?${suffix}` : pathname, {
      scroll: false,
    });
    // Runs when the address or the first answer arrives, and only then.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    wanted,
    missingList,
    agentsLoading,
    discovery.complete,
    wantedDetail.status,
  ]);

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
      <TelegramRemovedNotices events={data?.removed_bots ?? []} />
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
          {rows.map((row) =>
            row.target.kind === "agent" ? (
              <Fragment key={telegramTargetOf(row.target)}>
                {row === firstInstance &&
                discovery.feature.known &&
                discovery.feature.execution === "unavailable" ? (
                  <li data-testid="telegram-instance-notice">
                    <TelegramExecutionNotice
                      reason={discovery.feature.reason}
                    />
                  </li>
                ) : null}
                <TelegramInstanceSettingsRow
                  row={row}
                  complete={discovery.complete}
                  now={now}
                  replaced={row.bot ? replaced.has(row.bot.id) : false}
                  onDialog={setDialog}
                />
              </Fragment>
            ) : (
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
            ),
          )}
        </ul>
      )}
      {discovery.pending ? (
        <p className="text-muted-foreground text-sm">{t.common.loading}</p>
      ) : null}
      {discovery.error ? (
        <div className="space-y-2">
          <p role="alert">{strings.unavailable.loadError}</p>
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={() => void discovery.retry()}
          >
            {strings.action.tryAgain}
          </Button>
        </div>
      ) : null}
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
        <TelegramSettingsBlock
          key={JSON.stringify([user?.id, user?.system_role])}
          settings={data.settings}
          showMessageSettings={MESSAGE_SETTINGS_SHOWN}
        />
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
