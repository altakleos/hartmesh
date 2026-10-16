"use client";

import Link from "next/link";
import type { ReactNode } from "react";

import { AgentAvatar } from "@/components/workspace/instances/agent-avatar";
import { usePersonNames } from "@/components/workspace/person-name";
import type { AgentInstanceDetail } from "@/core/agent-instances/api";
import { useAuth } from "@/core/auth/AuthProvider";
import { useI18n } from "@/core/i18n/hooks";
import type { AgentThread } from "@/core/threads/types";

export function ConversationAgentHeader({
  binding,
  detail,
  title,
  alsoWorking,
}: {
  binding: NonNullable<AgentThread["agent_instance"]>;
  detail: AgentInstanceDetail | null;
  title: ReactNode;
  alsoWorking: boolean;
}) {
  const { user } = useAuth();
  const { t } = useI18n();
  const copy = t.agentInstances;
  const ids = detail
    ? [detail.owner_id, detail.supervisor.subject_id].filter(
        (id): id is string => !!id,
      )
    : [];
  const names = usePersonNames(ids, true);
  const subline = detail
    ? copy.chat.subline(
        binding.custody,
        detail.owner_id === user?.id,
        detail.owner_id ? (names.get(detail.owner_id) ?? "…") : "…",
        names.get(detail.supervisor.subject_id) ?? "…",
      )
    : binding.custody === "personal"
      ? copy.marker.aiAssistant
      : copy.marker.aiEmployee;
  return (
    <div className="flex min-w-0 items-center gap-3">
      <AgentAvatar instance={binding} />
      <div className="min-w-0">
        <div className="flex min-w-0 items-center gap-2">
          <Link
            className="truncate underline"
            href={`/workspace/instances?instance=${binding.id}`}
          >
            {binding.name}
          </Link>
          <div className="min-w-0 truncate">{title}</div>
        </div>
        <div className="text-muted-foreground flex flex-wrap gap-x-3 text-xs">
          <span>{subline}</span>
          {alsoWorking && <span>{copy.chat.alsoWorking}</span>}
        </div>
      </div>
    </div>
  );
}
