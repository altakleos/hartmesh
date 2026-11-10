"use client";

import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { useId } from "react";

import { Button } from "@/components/ui/button";
import { useSupervisorName } from "@/components/workspace/person-name";
import type {
  AgentInstance,
  AgentInstanceDetail,
} from "@/core/agent-instances/api";
import { hasCeiling } from "@/core/agent-instances/presentation";
import { useAuth } from "@/core/auth/AuthProvider";
import { useAgentInstances, useTelegram } from "@/core/features/hooks";
import { useI18n } from "@/core/i18n/hooks";
import {
  useTelegramBots,
  useTelegramClock,
  useTelegramReader,
} from "@/core/telegram-bots/hooks";
import { useTelegramConnectedCount } from "@/core/telegram-bots/instance-hooks";
import {
  instanceConnectBlock,
  type InstanceConnectBlock,
} from "@/core/telegram-bots/instance-state";
import { rowOf } from "@/core/telegram-bots/rows";
import { telegramText } from "@/core/telegram-bots/text";

import { TelegramExecutionNotice } from "./bot-row";
import { openTelegramSetup } from "./setup-dialog-store";

export function useInstanceTelegram(instance: AgentInstanceDetail) {
  const { user } = useAuth();
  const telegram = useTelegram();
  const feature = useAgentInstances();
  const list = useTelegramBots({ refetchInterval: 10_000 });
  const now = useTelegramClock();
  const reader = useTelegramReader();
  const bot =
    list.data?.bots.find(
      (bot) => bot.agent_kind === "instance" && bot.agent_ref === instance.id,
    ) ?? null;
  const execution =
    feature.execution === "unavailable"
      ? { execution: "unavailable" as const, reason: feature.reason }
      : instance.execution;
  const view = rowOf({
    instance,
    name: instance.name,
    bot,
    feature: telegram,
    reader,
    now,
    execution,
  });
  const block: InstanceConnectBlock | null =
    list.isError && !list.data
      ? { kind: "unreadable" }
      : instanceConnectBlock({
          instance,
          telegram,
          feature,
          bots: list.data,
          canRead: !!user && hasCeiling(user.permissions, "agents:read"),
        });
  return { telegram, bot, view, block };
}

function NoUse({ instance }: { instance: AgentInstance }) {
  const { t } = useI18n();
  const supervisor = useSupervisorName(instance.supervisor.subject_id);
  return <>{t.agentInstances.conversations.noUse(supervisor)}</>;
}

/** Shared explanatory content for every disabled instance Connect. */
export function TelegramConnectNotice({
  block,
  instance,
}: {
  block: InstanceConnectBlock;
  instance: AgentInstance;
}) {
  const { t } = useI18n();
  const reader = useTelegramReader();
  const { data } = useTelegramBots();
  const values = {
    ...reader,
    name: instance.name,
    count: data?.limits.used ?? 0,
  };
  switch (block.kind) {
    case "telegram":
      return <>{telegramText(t.telegramBots, block.key, values)}</>;
    case "execution":
      return (
        <TelegramExecutionNotice
          reason={block.reason}
          name={instance.name}
          custody={instance.custody}
        />
      );
    case "policy":
      return <>{t.telegramBots.state.notAllowedConnect(values)}</>;
    case "use":
      return <NoUse instance={instance} />;
    case "retired":
      return <>{t.agentInstances.retiredNotice(instance.name)}</>;
    case "personal-off":
      return <>{t.sidebar.assistantsOff}</>;
    case "company-off":
      return <>{t.sidebar.aiEmployeesOff}</>;
    case "limit":
      return <>{t.telegramBots.unavailable.limitPerson(values)}</>;
    case "ceiling":
      return <>{t.telegramBots.refusal.ceiling}</>;
    case "unreadable":
      return <>{t.telegramBots.unavailable.loadError}</>;
    case "loading":
      return <>{t.common.loading}</>;
  }
}

/** Header controls share the same current availability as Settings. */
export function TelegramInstanceConnect({
  instance,
}: {
  instance: AgentInstanceDetail;
}) {
  const { t } = useI18n();
  const { telegram, bot, view, block } = useInstanceTelegram(instance);
  const router = useRouter();
  const pathname = usePathname();
  const search = useSearchParams();
  const id = useId();
  if (!telegram.known) return null;
  if (bot && view.buttons.includes("action.open"))
    return (
      <Button asChild variant="outline">
        <a href={bot.url} target="_blank" rel="noopener noreferrer">
          {t.telegramBots.action.open}
        </a>
      </Button>
    );
  if (bot)
    return (
      <Button
        variant="outline"
        onClick={() => {
          const params = new URLSearchParams(search);
          params.set("settings", "channels");
          params.set("telegram", `agent.${instance.id}`);
          router.push(`${pathname}?${params.toString()}`);
        }}
      >
        {t.telegramBots.action.settings}
      </Button>
    );
  return (
    <div className="space-y-1">
      <Button
        variant="outline"
        disabled={!!block}
        aria-describedby={block ? id : undefined}
        onClick={() =>
          openTelegramSetup({
            kind: "agent",
            id: instance.id,
            name: instance.name,
            permissions: instance.permissions,
          })
        }
      >
        {t.telegramBots.action.connect}
      </Button>
      {block && (
        <div id={id} className="text-muted-foreground text-sm">
          <TelegramConnectNotice block={block} instance={instance} />
        </div>
      )}
    </div>
  );
}

export function TelegramInstanceState({
  instance,
}: {
  instance: AgentInstanceDetail;
}) {
  const { t } = useI18n();
  const { telegram, bot, view } = useInstanceTelegram(instance);
  if (!telegram.known || !bot) return null;
  return (
    <div className="space-y-1 text-sm">
      <span>{t.telegramBots.page.label}:</span>
      {view.lines.map((line) => (
        <p key={line.key}>
          {telegramText(t.telegramBots, line.key, line.values)}
        </p>
      ))}
      {view.execution && (
        <TelegramExecutionNotice
          reason={view.execution.reason}
          name={instance.name}
          custody={instance.custody}
        />
      )}
    </div>
  );
}

export function TelegramInstanceNote({
  instance,
  kind,
}: {
  instance: AgentInstance;
  kind: "rename" | "delete";
}) {
  const { t } = useI18n();
  const count = useTelegramConnectedCount(instance);
  if (count < 1) return null;
  return (
    <p className="text-muted-foreground text-sm">
      {kind === "rename"
        ? t.telegramBots.page.renameNote({ name: instance.name })
        : t.telegramBots.page.deleteNote({ name: instance.name, count })}
    </p>
  );
}

export function TelegramInstanceTip({
  instance,
}: {
  instance: AgentInstanceDetail;
}) {
  const { t } = useI18n();
  const { telegram, bot, block } = useInstanceTelegram(instance);
  if (!telegram.known || bot || block) return null;
  return (
    <p>
      <Button
        variant="link"
        className="h-auto p-0 whitespace-normal"
        onClick={() =>
          openTelegramSetup({
            kind: "agent",
            id: instance.id,
            name: instance.name,
            permissions: instance.permissions,
          })
        }
      >
        {t.telegramBots.offer.tip({ name: instance.name })}
      </Button>
    </p>
  );
}
