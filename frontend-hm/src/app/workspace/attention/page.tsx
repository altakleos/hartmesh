"use client";
import { useSearchParams } from "next/navigation";

import { AttentionInbox } from "@/components/workspace/attention/inbox";
import {
  WorkspaceBody,
  WorkspaceContainer,
  WorkspaceHeader,
} from "@/components/workspace/workspace-container";
import { useAgentsApiEnabled } from "@/core/agents";
import { useAuth } from "@/core/auth/AuthProvider";
import { useDocumentTitle, useStorageSpacesEnabled } from "@/core/features";
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
  const spaces = useStorageSpacesEnabled();
  const agents = useAgentsApiEnabled();
  useDocumentTitle(t.attention.title, t.pages.appName);
  const work = search.get("work") ?? undefined;
  const id = search.get("request") ?? undefined;
  return (
    <WorkspaceContainer>
      <WorkspaceHeader />
      <WorkspaceBody>
        <div className="mx-auto h-full w-full max-w-5xl space-y-4 overflow-y-auto p-6">
          <h1 className="text-2xl font-semibold">{t.attention.title}</h1>
          {spaces.enabled && agents.enabled && canRead ? (
            <AttentionInbox
              key={`${user?.id}:${user?.permissions?.join(",")}:${work}:${id}`}
              work={work}
              initialRequest={id}
            />
          ) : (
            <p>{t.attention.unavailable}</p>
          )}
        </div>
      </WorkspaceBody>
    </WorkspaceContainer>
  );
}
