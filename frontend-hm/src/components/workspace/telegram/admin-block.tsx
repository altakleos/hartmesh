"use client";

import { useQuery } from "@tanstack/react-query";

import { useI18n } from "@/core/i18n/hooks";
import { getTelegramAdminSummary } from "@/core/telegram-bots/api";
import { useTelegramReader } from "@/core/telegram-bots/hooks";

/** Counts and limits only: bots are personal, so no username stands here. */
export function TelegramAdminBlock() {
  const { t } = useI18n();
  const strings = t.telegramBots;
  const { provider } = useTelegramReader();
  const { data } = useQuery({
    queryKey: ["telegram-bots-admin"],
    queryFn: getTelegramAdminSummary,
  });
  if (!data) {
    return null;
  }
  const transcriber =
    data.voice_provider === "gemini"
      ? strings.transcriber.gemini
      : strings.transcriber.openai;
  return (
    <section className="space-y-2">
      <h3 className="font-medium">{strings.admin.title}</h3>
      <p className="text-sm">
        {data.bots === 0
          ? strings.admin.none
          : strings.admin.counts({
              bots: data.bots,
              people: data.people,
              polling: data.polling,
              failing: data.failing,
              held: data.held,
            })}
      </p>
      <p className="text-sm">
        {strings.admin.limits({
          perPerson: data.limits.per_person,
          limit: data.limits.deployment,
        })}
      </p>
      <p className="text-sm">
        {data.voice === "available"
          ? strings.admin.voiceOn({ transcriber })
          : strings.admin.voiceOff({ provider })}
      </p>
      <p className="text-muted-foreground text-xs">{strings.admin.private}</p>
    </section>
  );
}
