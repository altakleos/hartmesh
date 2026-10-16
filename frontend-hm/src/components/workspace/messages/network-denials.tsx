"use client";

import { useConversationFiles } from "@/components/workspace/chats/conversation-files";
import { ProviderSupportLink } from "@/components/workspace/instances/execution-notice";
import { useSupervisorName } from "@/components/workspace/person-name";
import { INSPECT, MANAGE } from "@/core/agent-instances/presentation";
import { useAuth } from "@/core/auth/AuthProvider";
import { useI18n } from "@/core/i18n/hooks";

export function NetworkDenials({ hosts }: { hosts: string[] }) {
  const { t } = useI18n();
  const { user } = useAuth();
  const { agent, detail, providerName, supportURL } = useConversationFiles();
  const supervisor = useSupervisorName(detail?.supervisor.subject_id ?? "");
  if (!agent) return null;
  const manager =
    !!detail &&
    (detail.permissions & (MANAGE | INSPECT)) === (MANAGE | INSPECT);
  const provider = providerName ?? t.agentInstances.providerFallback;
  const support =
    user?.system_role === "admin"
      ? provider
      : t.agentInstances.administratorSupport;
  return (
    <div className="space-y-2 px-4 pb-2 text-sm" data-testid="network-denials">
      {hosts.map((host) => (
        <p key={host}>
          {manager
            ? t.sandboxNetwork.denied(agent.name, host, provider)
            : t.sandboxNetwork.deniedAsk(agent.name, host, supervisor, support)}
        </p>
      ))}
      {!manager && user?.system_role === "admin" && (
        <ProviderSupportLink provider={providerName} supportURL={supportURL} />
      )}
    </div>
  );
}
