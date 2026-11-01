"use client";

import { Button } from "@/components/ui/button";
import { SidebarMenuItem } from "@/components/ui/sidebar";
import { ChannelProviderIcon } from "@/components/workspace/channels/channel-provider-icon";
import { openSettingsDialog } from "@/components/workspace/settings/settings-dialog-store";
import { useTelegram } from "@/core/features/hooks";
import { useI18n } from "@/core/i18n/hooks";
import { useTelegramBots, useTelegramReader } from "@/core/telegram-bots/hooks";
import { needsAttention, rowOf } from "@/core/telegram-bots/rows";
import { cn } from "@/lib/utils";

/** Rendered while Telegram is on, also when it cannot run: the section says why. */
export function TelegramSidebarRow() {
  const { t } = useI18n();
  const strings = t.telegramBots;
  const feature = useTelegram();
  const reader = useTelegramReader();
  const { data } = useTelegramBots();

  const bots = data?.bots ?? [];
  const now = Date.now();
  const attention = bots.filter((bot) =>
    needsAttention(
      rowOf({ name: bot.agent_name ?? "", bot, feature, reader, now }).row,
    ),
  ).length;

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
              : attention > 0
                ? strings.sidebar.attention({ count: attention })
                : strings.sidebar.connected({
                    count: bots.filter((bot) => bot.state === "connected")
                      .length,
                  })}
          </span>
        </Button>
      </div>
    </SidebarMenuItem>
  );
}
