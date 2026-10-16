"use client";

import { useQuery, useQueryClient } from "@tanstack/react-query";
import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import { useCallback, useEffect, useRef, useState } from "react";

import { AccessTab } from "@/components/workspace/instances/access-tab";
import {
  AgentHeader,
  AgentAvailabilityNotice,
} from "@/components/workspace/instances/agent-header";
import { ConversationsTab } from "@/components/workspace/instances/conversations-tab";
import { DeletedAgentNavigation } from "@/components/workspace/instances/delete-agent-dialog";
import { FilesTab } from "@/components/workspace/instances/files-tab";
import {
  LifecycleActionsProvider,
  LifecycleState,
  useLifecycleActions,
} from "@/components/workspace/instances/lifecycle-actions";
import { MemoryTab } from "@/components/workspace/instances/memory-tab";
import { SettingsTab } from "@/components/workspace/instances/settings-tab";
import { UnfinishedCard } from "@/components/workspace/instances/unfinished-card";
import { WorkPanel } from "@/components/workspace/instances/work-panel";
import { useSupervisorName } from "@/components/workspace/person-name";
import {
  WorkspaceBody,
  WorkspaceContainer,
  WorkspaceHeader,
} from "@/components/workspace/workspace-container";
import * as api from "@/core/agent-instances/api";
import {
  agentBlock,
  hasCeiling,
  INSPECT,
  MANAGE,
} from "@/core/agent-instances/presentation";
import { useAuth } from "@/core/auth/AuthProvider";
import { useAgentInstances, useDocumentTitle } from "@/core/features";
import { useI18n } from "@/core/i18n/hooks";

export default function InstancesPage() {
  const { user } = useAuth();
  const { t } = useI18n();
  const feature = useAgentInstances();
  const search = useSearchParams();
  const router = useRouter();
  const requested = search.get("instance");
  const selected =
    requested && /^[0-9a-f]{32}$/.test(requested) ? requested : null;
  const fallback = user?.system_role === "admin" ? "company" : "personal";
  const canRead = !!user && hasCeiling(user.permissions, "agents:read");
  const scope = JSON.stringify([
    user?.id,
    user?.system_role,
    user?.permissions,
    selected,
  ]);
  const [knownKind, setKnownKind] = useState<{
    scope: string;
    kind: api.Custody;
  }>();
  const reportKind = useCallback(
    (kind: api.Custody) => {
      setKnownKind((current) =>
        current?.scope === scope && current.kind === kind
          ? current
          : { scope, kind },
      );
    },
    [scope],
  );
  const kind = knownKind?.scope === scope ? knownKind.kind : null;
  useEffect(() => {
    if (!requested)
      router.replace(
        fallback === "company"
          ? "/workspace/employees"
          : "/workspace/assistants",
      );
  }, [requested, fallback, router]);
  return (
    <WorkspaceContainer>
      <WorkspaceHeader
        breadcrumbSection={{
          label:
            kind === "personal"
              ? t.agentInstances.assistants.title
              : kind === "company"
                ? t.agentInstances.employees.title
                : t.sidebar.agents,
          href:
            kind === "personal"
              ? "/workspace/assistants"
              : kind === "company"
                ? "/workspace/employees"
                : "/workspace/agents",
        }}
      />
      <WorkspaceBody>
        <div className="mx-auto h-full w-full max-w-5xl space-y-5 overflow-y-auto p-6">
          {feature.isLoading || !requested ? (
            <p role="status">{t.agentInstances.loading}</p>
          ) : !feature.known || !feature.enabled || !canRead ? (
            <p>{t.sidebar.agentsUnavailable}</p>
          ) : selected ? (
            <InstanceDetail
              key={`${user?.id}:${user?.system_role}:${user?.permissions?.join(",")}:${selected}`}
              id={selected}
              onKindChange={reportKind}
            />
          ) : (
            <NotFound kind={fallback} />
          )}
        </div>
      </WorkspaceBody>
    </WorkspaceContainer>
  );
}
function NotFound({ kind }: { kind: api.Custody }) {
  const { t } = useI18n();
  return (
    <>
      <p role="alert">{t.agentInstances.notFound}</p>
      <Link
        className="underline"
        href={
          kind === "personal" ? "/workspace/assistants" : "/workspace/employees"
        }
      >
        {t.agentInstances.backToList(kind)}
      </Link>
    </>
  );
}
function InstanceDetail({
  id,
  onKindChange,
}: {
  id: string;
  onKindChange: (kind: api.Custody) => void;
}) {
  const { user } = useAuth();
  const { t } = useI18n();
  const copy = t.agentInstances;
  const search = useSearchParams();
  const client = useQueryClient();
  const key = [
    "agent-instances",
    user?.id,
    user?.system_role,
    user?.permissions,
    id,
  ];
  const detail = useQuery({
    queryKey: [...key, "detail"],
    queryFn: ({ signal }) => api.getInstance(id, signal),
    refetchInterval: 10_000,
    refetchIntervalInBackground: false,
    retry: false,
    staleTime: 0,
  });
  const current = !detail.isError ? detail.data : undefined;
  const knownKind = useRef<api.Custody | undefined>(undefined);
  useEffect(() => {
    if (current) {
      knownKind.current = current.custody;
      onKindChange(current.custody);
    }
  }, [current, onKindChange]);
  useDocumentTitle(current?.name ?? t.sidebar.agents, t.pages.appName);
  const inspect = !!current && (current.permissions & INSPECT) !== 0;
  const manage = !!current && (current.permissions & MANAGE) !== 0;
  const readable =
    !!current &&
    current.status !== "deleted" &&
    current.status !== "provisioning";
  const [observeDeletion, setObserveDeletion] = useState(false);
  useEffect(() => {
    if (!observeDeletion) return;
    const timer = setTimeout(() => setObserveDeletion(false), 180_000);
    return () => clearTimeout(timer);
  }, [observeDeletion]);
  const lifecycle = useQuery({
    queryKey: [...key, "lifecycle", current?.permissions],
    queryFn: ({ signal }) => api.getInstanceLifecycle(id, signal),
    enabled: (readable || observeDeletion) && (inspect || manage),
    refetchInterval: 10_000,
    refetchIntervalInBackground: false,
    retry: false,
  });
  const adopted = useQuery({
    queryKey: [
      ...key,
      "definition",
      current?.definition_revision,
      current?.permissions,
    ],
    queryFn: ({ signal }) => api.getInstanceDefinition(id, signal),
    enabled: readable && inspect,
    retry: false,
  });
  const requested = search.get("work")
    ? "work"
    : (search.get("tab") ?? "conversations");
  const tabs = [
    "conversations",
    "files",
    "work",
    ...(inspect && hasCeiling(user?.permissions, "memory:read")
      ? ["memory"]
      : []),
    ...(manage ? ["access", "settings"] : []),
  ] as (keyof typeof copy.tabs)[];
  const tab = tabs.find((tab) => tab === requested) ?? "conversations";
  function refresh() {
    void client.invalidateQueries({ queryKey: ["agent-instances"] });
    void client.invalidateQueries({ queryKey: ["features"] });
  }
  if (detail.isPending) return <p role="status">{copy.loading}</p>;
  if (!current)
    return (
      <NotFound
        kind={
          knownKind.current ??
          (user?.system_role === "admin" ? "company" : "personal")
        }
      />
    );
  return (
    <LifecycleActionsProvider
      instance={current}
      observationEpoch={detail.dataUpdatedAt}
      onDeletePendingChange={setObserveDeletion}
      operations={lifecycle.data?.operations ?? []}
      refresh={refresh}
    >
      <InstanceContent
        current={current}
        tab={tab}
        tabs={tabs}
        definition={!adopted.isError ? adopted.data : undefined}
        operations={lifecycle.data?.operations}
        refresh={refresh}
      />
    </LifecycleActionsProvider>
  );
}
type Tab =
  | "conversations"
  | "files"
  | "work"
  | "memory"
  | "access"
  | "settings";
function InstanceContent({
  current,
  tab,
  tabs,
  definition,
  operations,
  refresh,
}: {
  current: api.AgentInstanceDetail;
  tab: Tab;
  tabs: Tab[];
  definition?: Awaited<ReturnType<typeof api.getInstanceDefinition>>;
  operations?: api.LifecycleOperation[];
  refresh: () => void;
}) {
  const { user } = useAuth();
  const { t } = useI18n();
  const copy = t.agentInstances;
  const search = useSearchParams();
  const feature = useAgentInstances();
  const { pending: changing } = useLifecycleActions();
  const inspect = (current.permissions & INSPECT) !== 0;
  const block = agentBlock(current, feature);
  const supervisor = useSupervisorName(current.supervisor.subject_id);
  return (
    <>
      <DeletedAgentNavigation />
      <AgentHeader
        changed={refresh}
        instance={current}
        lifecyclePending={changing}
        brief={definition?.soul}
      />
      <LifecycleState />
      {current.status === "deleted" ? (
        <>
          <p>{copy.deletedNotice(current.name)}</p>
          <Link
            className="underline"
            href={
              current.custody === "personal"
                ? "/workspace/assistants"
                : "/workspace/employees"
            }
          >
            {copy.backToList(current.custody)}
          </Link>
        </>
      ) : current.status === "provisioning" ? (
        <UnfinishedCard
          entry={current}
          executable={feature.execution === "ready"}
          notice={
            block ? (
              <AgentAvailabilityNotice instance={current} block={block} />
            ) : undefined
          }
          refresh={refresh}
        />
      ) : (
        <>
          {search.get("employeeAdded") === "1" && (
            <p role="status">{copy.settings.employeeAdded(current.name)}</p>
          )}
          {search.get("added") === "1" && (
            <p role="status">{copy.create.addedNoConversation(current.name)}</p>
          )}
          <nav
            role="tablist"
            aria-label={current.name}
            className="flex flex-wrap gap-2 border-b pb-2"
          >
            {tabs.map((item) => (
              <Link
                role="tab"
                aria-selected={tab === item}
                key={item}
                className="aria-selected:bg-muted rounded px-3 py-2 text-sm"
                href={`/workspace/instances?instance=${current.id}&tab=${item}`}
              >
                {copy.tabs[item]}
              </Link>
            ))}
          </nav>
          <div role="tabpanel" aria-label={copy.tabs[tab]}>
            {tab === "conversations" && <ConversationsTab instance={current} />}
            {tab === "files" && (
              <FilesTab
                instance={current}
                changed={refresh}
                initialFolder={search.get("folder") ?? undefined}
              />
            )}
            {tab === "work" &&
              (inspect ? (
                <WorkPanel
                  instance={current}
                  unavailable={block}
                  policy={definition?.config.work_policy}
                  canRead={hasCeiling(user?.permissions, "agents:read")}
                  canWrite={hasCeiling(user?.permissions, "agents:write")}
                  canActivate={hasCeiling(user?.permissions, "runs:create")}
                  canCreateConversation={hasCeiling(
                    user?.permissions,
                    "threads:write",
                  )}
                  canStop={hasCeiling(user?.permissions, "runs:cancel")}
                  initialWork={search.get("work")}
                  lifecyclePending={changing}
                  lifecycleOperations={operations}
                />
              ) : (
                <p>{t.agentWork.visibility(supervisor)}</p>
              ))}
            {tab === "memory" && (
              <MemoryTab instance={current} lifecyclePending={changing} />
            )}
            {tab === "access" && <AccessTab />}
            {tab === "settings" && <SettingsTab />}
          </div>
        </>
      )}
    </>
  );
}
