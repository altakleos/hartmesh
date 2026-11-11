"use client";

import { hasCeiling } from "@/core/agent-instances/presentation";
import { useAuth } from "@/core/auth/AuthProvider";
import { useAgentInstances, useTelegram } from "@/core/features/hooks";
import { useI18n } from "@/core/i18n/hooks";
import { useTelegramBots, useTelegramReader } from "@/core/telegram-bots/hooks";
import { useTelegramInstanceDetail } from "@/core/telegram-bots/instance-hooks";
import {
  instanceConnectBlock,
  type InstanceConnectBlock,
} from "@/core/telegram-bots/instance-state";
import { rowOf, type TargetRow } from "@/core/telegram-bots/rows";

import { TelegramBotRow } from "./bot-row";
import { TelegramConnectNotice } from "./instance-controls";
import type { TelegramRowDialogState } from "./row-dialogs";

/** Current detail supplies permissions and lifecycle as well as execution. */
export function TelegramInstanceSettingsRow({
  row,
  complete,
  now,
  replaced,
  onDialog,
}: {
  row: TargetRow;
  complete: boolean;
  now: number;
  replaced: boolean;
  onDialog: (dialog: TelegramRowDialogState) => void;
}) {
  const { user } = useAuth();
  const { t } = useI18n();
  const telegram = useTelegram();
  const feature = useAgentInstances();
  const reader = useTelegramReader();
  const bots = useTelegramBots();
  const detail = useTelegramInstanceDetail(row.instance);
  const current =
    detail.data !== undefined
      ? detail.data
      : (row.instance ?? (complete ? null : undefined));
  const displayed = current
    ? {
        ...row,
        name: current.name,
        instance: current,
        target: {
          kind: "agent" as const,
          id: current.id,
          name: current.name,
          permissions: current.permissions,
        },
      }
    : row;
  const block: InstanceConnectBlock | null = detail.isError
    ? { kind: "unreadable" }
    : row.instance && detail.isPending
      ? { kind: "loading" }
      : current
        ? instanceConnectBlock({
            instance: current,
            telegram,
            feature,
            bots: bots.data,
            canRead: !!user && hasCeiling(user.permissions, "agents:read"),
          })
        : { kind: "use" };
  return (
    <TelegramBotRow
      row={displayed}
      now={now}
      onDialog={onDialog}
      disabledBecause={
        block ? (
          current ? (
            <TelegramConnectNotice block={block} instance={current} />
          ) : (
            t.telegramBots.refusal.agentGone({ name: row.name })
          )
        ) : null
      }
      view={rowOf({
        name: displayed.name,
        bot: row.bot,
        feature: telegram,
        reader,
        now,
        replaced,
        instance: current,
        execution:
          feature.execution === "unavailable"
            ? { execution: "unavailable", reason: feature.reason }
            : detail.data?.execution,
      })}
    />
  );
}
