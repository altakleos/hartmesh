"use client";

import { Button } from "@/components/ui/button";
import { SidebarMenuItem } from "@/components/ui/sidebar";
import { ChannelProviderIcon } from "@/components/workspace/channels/channel-provider-icon";
import { openSettingsDialog } from "@/components/workspace/settings/settings-dialog-store";
import { useTelegram } from "@/core/features/hooks";
import { useI18n } from "@/core/i18n/hooks";
import {
  useTelegramBots,
  useTelegramClock,
  useTelegramReader,
} from "@/core/telegram-bots/hooks";
import { useTelegramInstanceDetails } from "@/core/telegram-bots/instance-hooks";
import {
  needsAttention,
  rowOf,
  sectionNoticeOf,
  shortStateOf,
} from "@/core/telegram-bots/rows";
import { cn } from "@/lib/utils";

/** Rendered while Telegram is on, also when it cannot run: the section says why. */
export function TelegramSidebarRow() {
  const { t } = useI18n();
  const strings = t.telegramBots;
  const feature = useTelegram();
  const reader = useTelegramReader();
  const { data } = useTelegramBots();
  const now = useTelegramClock();

  const bots = data?.bots ?? [];
  const instanceBots = bots.filter((bot) => bot.agent_kind === "instance");
  const details = useTelegramInstanceDetails(
    instanceBots.map((bot) => ({ id: bot.agent_ref })),
  );
  const instances = new Map(
    instanceBots.map((bot, index) => [bot.agent_ref, details[index]?.data]),
  );
  const rows = bots.map(
    (bot) =>
      rowOf({
        name: bot.agent_name ?? "",
        bot,
        feature,
        reader,
        now,
        instance:
          bot.agent_kind === "instance"
            ? instances.get(bot.agent_ref)
            : undefined,
      }).row,
  );
  const paused = sectionNoticeOf(feature, true) !== null;
  const attention = paused ? 0 : rows.filter(needsAttention).length;
  // A connected bot stays connected through an outage, which is no one's to fix.
  const connected = bots.filter((bot) => bot.state === "connected").length;
  const waiting = rows.some((row) => shortStateOf(row) === "short.waiting");

  return (
    <SidebarMenuItem>
      <div className="hover:bg-sidebar-accent flex h-10 items-center gap-2 rounded-md px-2">
        <ChannelProviderIcon provider="telegram" className="size-5 shrink-0" />
        <span className="min-w-0 flex-1 truncate text-sm font-medium">
          {strings.sidebar.label}
        </span>
        <Button
          type="button"
          size="sm"
          variant={bots.length === 0 ? "secondary" : "outline"}
          className={cn(
            "h-8 w-24 px-2 text-xs",
            attention > 0 && "text-amber-600 dark:text-amber-400",
          )}
          onClick={() => openSettingsDialog("channels")}
        >
          <span className="truncate">
            {bots.length === 0
              ? strings.sidebar.connect
              : paused
                ? strings.sidebar.paused
                : attention > 0
                  ? strings.sidebar.attention({ count: attention })
                  : waiting && connected === 0
                    ? strings.sidebar.waiting
                    : strings.sidebar.connected({ count: connected })}
          </span>
        </Button>
      </div>
    </SidebarMenuItem>
  );
}
