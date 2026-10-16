"use client";
import { useSearchParams } from "next/navigation";

import { Button } from "@/components/ui/button";
import { AttentionExecutionBanner } from "@/components/workspace/attention/execution-banner";
import { AttentionInbox } from "@/components/workspace/attention/inbox";
import {
  WorkspaceBody,
  WorkspaceContainer,
  WorkspaceHeader,
} from "@/components/workspace/workspace-container";
import { useAuth } from "@/core/auth/AuthProvider";
import { useAgentInstances, useDocumentTitle } from "@/core/features";
import { useI18n } from "@/core/i18n/hooks";

export default function AttentionPage() {
  const { user } = useAuth();
  const { t } = useI18n();
  const search = useSearchParams();
  const canRead =
    !!user &&
    (!user.permissions ||
      user.permissions.some((p) =>
        ["*", "agents:*", "agents:read"].includes(p),
      ));
  const agents = useAgentInstances();
  useDocumentTitle(t.attention.title, t.pages.appName);
  const work = search.get("work") ?? undefined;
  const id = search.get("request") ?? undefined;
  return (
    <WorkspaceContainer>
      <WorkspaceHeader />
      <WorkspaceBody>
        <div className="mx-auto h-full w-full max-w-5xl space-y-4 overflow-y-auto p-6">
          <h1 className="text-2xl font-semibold">{t.attention.title}</h1>
          {agents.isLoading ? (
            <p role="status">{t.agentInstances.loading}</p>
          ) : !canRead ? (
            <p>{t.attention.noPermission}</p>
          ) : !agents.known || !agents.enabled ? (
            <div>
              <p>{t.sidebar.agentsUnavailable}</p>
              <Button onClick={() => void agents.refetch()}>
                {t.attention.refresh}
              </Button>
            </div>
          ) : (
            <>
              {agents.execution === "unavailable" && (
                <AttentionExecutionBanner feature={agents} />
              )}
              <AttentionInbox
                executionUnavailable={agents.execution === "unavailable"}
                key={`${user?.id}:${user?.system_role}:${user?.permissions?.join(",")}:${work}:${id}`}
                work={work}
                initialRequest={id}
              />
            </>
          )}
        </div>
      </WorkspaceBody>
    </WorkspaceContainer>
  );
}
