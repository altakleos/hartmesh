"use client";

import { useSearchParams } from "next/navigation";

import { Button } from "@/components/ui/button";
import { useTelegram } from "@/core/features/hooks";
import { useI18n } from "@/core/i18n/hooks";
import { useLocalSettings } from "@/core/settings";
import { useTelegramBots, useTelegramReader } from "@/core/telegram-bots/hooks";
import { sectionNoticeOf } from "@/core/telegram-bots/rows";
import { telegramText } from "@/core/telegram-bots/text";

import { openTelegramSetup } from "./setup-dialog-store";

/** The new-chat page offers the ordinary chat's bot once, until Not now. */
export function TelegramWelcomeCard() {
  const { t } = useI18n();
  const strings = t.telegramBots;
  const feature = useTelegram();
  const reader = useTelegramReader();
  const { data } = useTelegramBots();
  const [settings, setSettings] = useLocalSettings();
  const isSkillMode = useSearchParams().get("mode") === "skill";

  const hasChatBot = data?.bots.some((bot) => bot.agent_kind === "lead_agent");
  if (
    !feature.known ||
    !feature.enabled ||
    data === undefined ||
    hasChatBot ||
    settings.telegram.offerDismissed ||
    isSkillMode
  ) {
    return null;
  }

  const notice = sectionNoticeOf(feature, false);
  const atLimit = data.limits.used >= data.limits.per_person;
  const blocked = notice
    ? telegramText(strings, notice.key, reader)
    : atLimit
      ? strings.unavailable.limitPerson({ count: data.limits.used })
      : null;

  return (
    <section
      className="mt-4 w-full max-w-md rounded-lg border p-4 text-left"
      data-testid="telegram-welcome-card"
    >
      <h2 className="font-medium">{strings.offer.chatTitle}</h2>
      <p className="text-muted-foreground mt-1 text-sm">
        {strings.offer.chatBody}
      </p>
      <div className="mt-3 flex gap-2">
        <Button
          type="button"
          size="sm"
          disabled={blocked !== null}
          onClick={() => openTelegramSetup({ kind: "chat" })}
        >
          {strings.action.connect}
        </Button>
        <Button
          type="button"
          size="sm"
          variant="ghost"
          onClick={() => setSettings("telegram", { offerDismissed: true })}
        >
          {strings.offer.later}
        </Button>
      </div>
      {blocked ? (
        <p className="text-muted-foreground mt-2 text-xs">{blocked}</p>
      ) : null}
    </section>
  );
}
