"use client";

import { useEffect, useRef, useState } from "react";

import { useSettingsDialog } from "@/components/workspace/settings/settings-dialog-store";
import { useAuth } from "@/core/auth/AuthProvider";
import { useI18n } from "@/core/i18n/hooks";
import { useSpaceActionSignal } from "@/core/spaces/lifetime";
import {
  markTelegramRemovalSeen,
  type RemovedBot,
} from "@/core/telegram-bots/api";

/** A list read alone never consumes a notice; visible Settings owns it. */
export function TelegramRemovedNotices({ events }: { events: RemovedBot[] }) {
  const { user } = useAuth();
  const settings = useSettingsDialog();
  if (!user || !settings.open || settings.section !== "channels") return null;
  return <Notices key={`${user.id}:${user.system_role}`} events={events} />;
}

const known = (events: RemovedBot[]) =>
  events.filter(
    (event) =>
      event.reason === "agent-deleted" && event.agent_kind === "instance",
  );

function Notices({ events }: { events: RemovedBot[] }) {
  const { t } = useI18n();
  const [shown, setShown] = useState(() => known(events));
  const [visible, setVisible] = useState(
    () => document.visibilityState === "visible",
  );
  const sent = useRef(new Set<string>());
  const capture = useSpaceActionSignal();
  useEffect(() => {
    setShown((previous) => {
      const ids = new Set(previous.map((event) => event.id));
      const added = known(events).filter((event) => !ids.has(event.id));
      return added.length ? [...previous, ...added] : previous;
    });
  }, [events]);
  useEffect(() => {
    const change = () => setVisible(document.visibilityState === "visible");
    document.addEventListener("visibilitychange", change);
    return () => document.removeEventListener("visibilitychange", change);
  }, []);
  useEffect(() => {
    if (!visible || document.visibilityState !== "visible") return;
    const signal = capture();
    for (const event of shown) {
      if (signal.aborted || sent.current.has(event.id)) continue;
      sent.current.add(event.id);
      // A missing receipt is already consumed; a failed acknowledgement may be
      // shown again next time. Keep this rendered sentence until settings closes.
      void markTelegramRemovalSeen(event.id, signal).catch(() => undefined);
    }
  }, [shown, visible, capture]);
  return (
    <>
      {shown.map((event) => (
        <p
          key={event.id}
          className="rounded-lg border p-3 text-sm"
          role="status"
        >
          {t.telegramBots.notice.agentDeleted({
            name: event.agent_name ?? t.telegramBots.kind.assistant({}),
          })}
        </p>
      ))}
    </>
  );
}
