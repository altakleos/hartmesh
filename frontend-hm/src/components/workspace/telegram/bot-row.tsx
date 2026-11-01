"use client";

import { EllipsisVerticalIcon } from "lucide-react";
import { useId, useState } from "react";
import { toast } from "sonner";

import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { ProviderSupportLink } from "@/components/workspace/instances/execution-notice";
import {
  closeConnectWindow,
  openConnectUrl,
  prepareConnectWindow,
} from "@/core/channels/open-connect-url";
import { useBranding } from "@/core/features/hooks";
import { useI18n } from "@/core/i18n/hooks";
import {
  applyTelegramProfile,
  type Bot,
  requestTelegramConnect,
  retryTelegramBot,
  TelegramRefusal,
} from "@/core/telegram-bots/api";
import { useRefreshTelegramBots } from "@/core/telegram-bots/hooks";
import type { RowView, TargetRow } from "@/core/telegram-bots/rows";
import { type TelegramKey, telegramText } from "@/core/telegram-bots/text";
import { displayNameOf } from "@/core/telegram-bots/username";
import { telegramTargetOf } from "@/core/telegram-bots/web-pages";

import { BotPicture } from "./bot-picture";
import type { TelegramRowDialogState } from "./row-dialogs";
import {
  holdTelegramConnect,
  openTelegramSetup,
  telegramConnectEpoch,
  useTelegramPageState,
} from "./setup-dialog-store";
import { useTelegramFailure } from "./use-telegram-failure";

const NAME_MAX = 64;
const MENU_HIDDEN_ROWS = new Set([1, 5]);
/** A name can be one long word, which a button would hold on one line past the screen. */
const WRAPS =
  "h-auto min-h-8 max-w-full py-1 whitespace-normal [overflow-wrap:anywhere]";

function menuOf(bot: Bot, row: number): TelegramKey[] {
  const live = !MENU_HIDDEN_ROWS.has(row);
  return [
    ...(live ? (["action.pasteToken"] as const) : []),
    ...(live && bot.health === "ok" ? (["action.applyProfile"] as const) : []),
    ...(bot.state === "connected" || bot.state === "blocked"
      ? (["action.otherAccount"] as const)
      : []),
    "action.remove",
  ];
}

export function TelegramBotRow({
  row,
  view,
  disabledBecause,
  now,
  onDialog,
}: {
  row: TargetRow;
  view: RowView;
  /** Why a Connect cannot be pressed, shown under it. */
  disabledBecause: string | null;
  now: number;
  onDialog: (dialog: TelegramRowDialogState) => void;
}) {
  const { t } = useI18n();
  const strings = t.telegramBots;
  const { target, name, bot } = row;
  const { connects } = useTelegramPageState();
  const { providerName, supportURL } = useBranding();
  const refresh = useRefreshTelegramBots();
  const failure = useTelegramFailure();
  const [busy, setBusy] = useState(false);
  const blockedId = useId();

  const held = bot ? connects.get(bot.id) : undefined;
  const liveConnect =
    held && Date.parse(held.expires_at) > now ? held : undefined;

  const newConnect = async (keepInPage: boolean) => {
    if (!bot) {
      return;
    }
    const tab = keepInPage ? null : prepareConnectWindow();
    const epoch = telegramConnectEpoch();
    try {
      const connect = await requestTelegramConnect(bot.id);
      if (!holdTelegramConnect(bot.id, connect, epoch)) {
        closeConnectWindow(tab);
      } else if (!keepInPage) {
        openConnectUrl(connect.url, tab);
      }
    } catch (error) {
      closeConnectWindow(tab);
      if (
        !(error instanceof TelegramRefusal) ||
        error.reason !== "already-connected"
      ) {
        failure.show(error, name);
      }
    }
    await refresh();
  };

  const run = async (key: TelegramKey) => {
    if (key === "action.connect") {
      openTelegramSetup(target);
      return;
    }
    if (!bot) {
      return;
    }
    const dialogs: Partial<Record<TelegramKey, TelegramRowDialogState>> = {
      "action.pasteToken": { kind: "token", bot, name },
      "action.otherAccount": { kind: "otherAccount", bot, name },
      "action.remove": { kind: "remove", bot, name },
      "action.thatWasMe": { kind: "thatWasMe", bot, name },
      "action.useHere": { kind: "useHere", bot, name },
    };
    const dialog = dialogs[key];
    if (dialog) {
      onDialog(dialog);
      return;
    }
    setBusy(true);
    try {
      if (key === "action.reconnect" || key === "action.openChat") {
        await newConnect(false);
      } else if (key === "action.newLink") {
        await newConnect(true);
      } else if (key === "action.tryNow") {
        await retryTelegramBot(bot.id);
        await refresh();
      } else if (key === "action.applyProfile") {
        await applyTelegramProfile(bot.id);
        toast.success(strings.toast.profileApplied);
        await refresh();
      }
    } catch (error) {
      failure.show(error, name);
    } finally {
      setBusy(false);
    }
  };

  const linkOf = (key: TelegramKey): string | null => {
    if (key === "action.open" && bot) {
      return bot.url;
    }
    if (key === "action.openChat" && bot) {
      return bot.bind_mode === "creator" ? bot.url : (liveConnect?.url ?? null);
    }
    return null;
  };

  const label = (key: TelegramKey) => telegramText(strings, key, { name });

  return (
    <li
      className="flex flex-wrap items-start gap-3 rounded-lg border p-3"
      data-telegram-row={view.row}
      data-telegram-target={telegramTargetOf(target)}
      tabIndex={-1}
    >
      <BotPicture target={target} />
      <div className="min-w-40 flex-1 space-y-1">
        <div className="font-medium break-words">{name}</div>
        {Array.from(name).length > NAME_MAX ? (
          <p className="text-muted-foreground text-xs">
            {strings.profile.nameCut({ shortened: displayNameOf(name) })}
          </p>
        ) : null}
        {target.kind === "custom_agent" ? (
          <p className="text-muted-foreground text-xs">
            {strings.kind.custom_agent}
          </p>
        ) : null}
        {bot ? (
          <p className="text-muted-foreground text-xs break-all">
            @{bot.bot_username}
          </p>
        ) : null}
        {view.lines.map((line) => (
          <p key={line.key} className="text-sm">
            {telegramText(strings, line.key, line.values)}
          </p>
        ))}
        {view.row === 13 && view.lines[0]?.values.admin ? (
          <ProviderSupportLink
            provider={providerName}
            supportURL={supportURL}
          />
        ) : null}
        {bot?.state === "connected" ? (
          <p className="text-muted-foreground text-xs">
            {bot.last_update_at
              ? strings.row.lastUsed({
                  time: new Date(bot.last_update_at).toLocaleString(),
                })
              : strings.row.neverUsed}
          </p>
        ) : null}
        {disabledBecause && !bot ? (
          <p id={blockedId} className="text-muted-foreground text-xs">
            {disabledBecause}
          </p>
        ) : null}
      </div>
      <div className="flex max-w-full flex-wrap items-center gap-2">
        {view.buttons.map((key) => {
          const href = linkOf(key);
          return href ? (
            <Button
              key={key}
              asChild
              size="sm"
              variant="outline"
              className={WRAPS}
            >
              <a href={href} target="_blank" rel="noopener noreferrer">
                {label(key)}
              </a>
            </Button>
          ) : (
            <Button
              key={key}
              type="button"
              size="sm"
              variant="outline"
              className={WRAPS}
              disabled={busy || (key === "action.connect" && !!disabledBecause)}
              aria-describedby={
                key === "action.connect" && disabledBecause
                  ? blockedId
                  : undefined
              }
              onClick={() => void run(key)}
            >
              {label(key)}
            </Button>
          );
        })}
        {bot ? (
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button
                type="button"
                size="icon-sm"
                variant="ghost"
                aria-label={strings.row.menu({ name })}
              >
                <EllipsisVerticalIcon className="size-4" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              {menuOf(bot, view.row).map((key) => (
                <DropdownMenuItem key={key} onSelect={() => void run(key)}>
                  {label(key)}
                </DropdownMenuItem>
              ))}
            </DropdownMenuContent>
          </DropdownMenu>
        ) : null}
      </div>
    </li>
  );
}
