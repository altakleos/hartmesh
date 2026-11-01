"use client";

import { useRouter } from "next/navigation";

import { Button } from "@/components/ui/button";
import { ChannelProviderIcon } from "@/components/workspace/channels/channel-provider-icon";
import { useTelegram } from "@/core/features/hooks";
import { useI18n } from "@/core/i18n/hooks";
import { useTelegramBots, useTelegramReader } from "@/core/telegram-bots/hooks";
import {
  rowOf,
  sectionNoticeOf,
  shortStateOf,
} from "@/core/telegram-bots/rows";
import { telegramText } from "@/core/telegram-bots/text";
import { telegramTargetOf } from "@/core/telegram-bots/web-pages";

import { openTelegramSetup } from "./setup-dialog-store";

/** One icon button on a custom agent's card, which says the state of its bot. */
export function TelegramAgentCardButton({ name }: { name: string }) {
  const { t } = useI18n();
  const strings = t.telegramBots;
  const router = useRouter();
  const feature = useTelegram();
  const reader = useTelegramReader();
  const { data } = useTelegramBots();

  if (!feature.known) {
    return null;
  }

  const target = { kind: "custom_agent", name } as const;
  const bot =
    data?.bots.find(
      (item) => item.agent_kind === "custom_agent" && item.agent_ref === name,
    ) ?? null;
  const short = (key: Parameters<typeof telegramText>[1]) =>
    strings.card.state({ state: telegramText(strings, key) });

  if (bot) {
    const { row } = rowOf({ name, bot, feature, reader, now: Date.now() });
    return (
      <Button
        size="icon"
        variant="ghost"
        className="h-8 w-8 shrink-0"
        title={short(shortStateOf(row))}
        onClick={() =>
          router.push(
            `/workspace/agents?settings=channels&telegram=${telegramTargetOf(target)}`,
          )
        }
      >
        <ChannelProviderIcon provider="telegram" className="size-3.5" />
      </Button>
    );
  }

  const blocked =
    sectionNoticeOf(feature, false) !== null ||
    (data !== undefined && data.limits.used >= data.limits.per_person);
  return (
    <Button
      size="icon"
      variant="ghost"
      className="h-8 w-8 shrink-0"
      disabled={blocked}
      title={
        blocked
          ? short(feature.enabled ? "short.none" : "short.off")
          : strings.action.connect
      }
      onClick={() => openTelegramSetup(target)}
    >
      <ChannelProviderIcon provider="telegram" className="size-3.5" />
    </Button>
  );
}
