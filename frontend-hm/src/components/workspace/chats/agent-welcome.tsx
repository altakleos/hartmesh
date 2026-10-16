"use client";

import { useQuery } from "@tanstack/react-query";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { useEffect, useRef, useState } from "react";

import { usePromptInputController } from "@/components/ai-elements/prompt-input";
import { AgentAvatar } from "@/components/workspace/instances/agent-avatar";
import {
  getInstanceDefinition,
  type AgentInstanceDetail,
} from "@/core/agent-instances/api";
import {
  hasCeiling,
  INSPECT,
  MANAGE,
} from "@/core/agent-instances/presentation";
import { useAuth } from "@/core/auth/AuthProvider";
import { useI18n } from "@/core/i18n/hooks";
import type { AgentThread } from "@/core/threads/types";

type Binding = NonNullable<AgentThread["agent_instance"]>;
export function AgentWelcome({
  binding,
  detail,
  threadId,
}: {
  binding: Binding;
  detail: AgentInstanceDetail | null;
  threadId: string;
}) {
  const { user } = useAuth();
  const { t } = useI18n();
  const copy = t.agentInstances.welcome;
  const search = useSearchParams();
  const { textInput } = usePromptInputController();
  const consumed = useRef<string | null>(null);
  const [tookWelcome, setTookWelcome] = useState(false);
  const canInspect =
    !!detail &&
    !!(detail.permissions & INSPECT) &&
    hasCeiling(user?.permissions, "agents:read");
  const definition = useQuery({
    queryKey: [
      "conversation-agent-definition",
      user?.id,
      user?.system_role,
      user?.permissions,
      binding.id,
      detail?.definition_revision,
      detail?.permissions,
    ],
    queryFn: ({ signal }) => getInstanceDefinition(binding.id, signal),
    enabled: canInspect && !!user,
    staleTime: 0,
    gcTime: 0,
    retry: false,
  });
  const firstMessage = copy.firstMessage(binding.name);
  useEffect(() => {
    if (
      search.get("welcome") !== "1" ||
      consumed.current === threadId ||
      window.location.pathname !==
        `/workspace/chats/${encodeURIComponent(threadId)}`
    )
      return;
    consumed.current = threadId;
    if (!textInput.value.trim()) {
      textInput.setInput(firstMessage);
      setTookWelcome(true);
    }
    const url = new URL(window.location.href);
    url.searchParams.delete("welcome");
    history.replaceState(null, "", url.pathname + url.search + url.hash);
  }, [search, threadId, firstMessage, textInput]);
  const description =
    canInspect && !definition.isError
      ? definition.data?.config.description
      : null;
  return (
    <section className="mx-auto flex w-full max-w-2xl flex-col gap-3 overflow-y-auto px-6 pt-20 pb-6">
      <div className="flex items-center gap-3">
        <AgentAvatar instance={binding} />
        <h1 className="text-xl font-semibold">{copy.title(binding.name)}</h1>
      </div>
      {description && <p>{description}</p>}
      <p>{copy.body(binding.name)}</p>
      <p>{copy.ownFiles(binding.name)}</p>
      {binding.custody === "personal" && (
        <p>{copy.assistantNoMyFiles(binding.name)}</p>
      )}
      <p>{copy.sharedReadable(binding.name)}</p>
      {tookWelcome && textInput.value === firstMessage && (
        <p role="status">{copy.prefilled}</p>
      )}
      <ul className="space-y-2">
        <li>{copy.tipFiles(binding.name)}</li>
        <li>
          <Link
            className="underline"
            href={`/workspace/instances?instance=${binding.id}&tab=work`}
          >
            {copy.tipWork(binding.name)}
          </Link>
        </li>
        {!!detail && !!(detail.permissions & MANAGE) && (
          <li>
            <Link
              className="underline"
              href={`/workspace/instances?instance=${binding.id}&tab=access`}
            >
              {copy.tipAccess}
            </Link>
          </li>
        )}
      </ul>
      {binding.custody === "company" && (
        <p>{copy.companyNotice(binding.name)}</p>
      )}
    </section>
  );
}
