"use client";

import { useInfiniteQuery, useQueryClient } from "@tanstack/react-query";
import Link from "next/link";
import { useEffect, useId, useMemo, useState } from "react";

import { Button } from "@/components/ui/button";
import { usePersonName } from "@/components/workspace/person-name";
import {
  WorkspaceBody,
  WorkspaceContainer,
  WorkspaceHeader,
} from "@/components/workspace/workspace-container";
import * as api from "@/core/agent-instances/api";
import {
  clearCreation,
  matchesCreation,
  readCreation,
} from "@/core/agent-instances/unfinished";
import { useAuth } from "@/core/auth/AuthProvider";
import {
  useAgentInstances,
  useBranding,
  useDocumentTitle,
  useStorage,
} from "@/core/features";
import { useI18n } from "@/core/i18n/hooks";

import { AgentAvatar } from "./agent-avatar";
import { CreateAssistant, CreateEmployee } from "./create-agent";
import { DeletedAgentNotice } from "./deleted-agent-notice";
import { ExecutionNotice, ProviderSupportLink } from "./execution-notice";
import { UnfinishedCard } from "./unfinished-card";

export function AgentList({ kind }: { kind: api.Custody }) {
  const { user } = useAuth();
  return (
    <ScopedAgentList
      key={`${user?.id}:${user?.system_role}:${user?.permissions?.join(",")}:${kind}`}
      kind={kind}
    />
  );
}

function ScopedAgentList({ kind }: { kind: api.Custody }) {
  const { user } = useAuth();
  const { t } = useI18n();
  const copy = t.agentInstances;
  const personal = kind === "personal";
  const strings = personal ? copy.assistants : copy.employees;
  const feature = useAgentInstances();
  const branding = useBranding();
  const storage = useStorage();
  const client = useQueryClient();
  const administrator = user?.system_role === "admin";
  const canRead =
    !!user &&
    (user.permissions == null ||
      user.permissions.some((p) =>
        ["*", "agents:*", "agents:read"].includes(p),
      ));
  const canWrite =
    !!user &&
    (user.permissions == null ||
      user.permissions.some((p) =>
        ["*", "agents:*", "agents:write"].includes(p),
      ));
  const enabled = feature.known && feature.enabled && canRead;
  const [creating, setCreating] = useState(false);
  const hintId = useId();
  useDocumentTitle(strings.title, t.pages.appName);
  const list = useInfiniteQuery({
    queryKey: [
      "agent-instances",
      user?.id,
      user?.system_role,
      user?.permissions,
      kind,
    ],
    enabled,
    initialPageParam: 0,
    queryFn: ({ pageParam, signal }) =>
      api.listInstances(kind, pageParam, signal),
    getNextPageParam: (page, _pages, offset) =>
      page.instances.length === 100 ? offset + 100 : undefined,
    retry: false,
  });
  const pages = enabled && !list.isError ? list.data?.pages : undefined;
  const entries =
    pages
      ?.flatMap((page) => page.instances)
      .filter(
        (entry) =>
          entry.status !== "deleted" && entry.status !== "provisioning",
      ) ?? [];
  const unfinished = useMemo(() => pages?.[0]?.unfinished ?? [], [pages]);
  const own = entries.filter(
    (entry) =>
      personal && api.isFullInstance(entry) && entry.owner_id === user?.id,
  );
  const others = personal
    ? entries.filter((entry) => !own.includes(entry))
    : [];
  const ownName = own[0]?.name ?? "";
  const unfinishedName = unfinished[0]?.name;
  const assistants = pages?.[0]?.ai_assistants;
  const perUser = assistants?.per_user ?? feature.assistants.perUser;
  const mine = assistants?.mine ?? feature.assistants.mine;
  const provider = branding.providerName ?? copy.providerFallback;
  const held = feature.execution === "unavailable";
  let row: number;
  if (feature.isLoading) row = 0;
  else if (!feature.known || !feature.enabled) row = 1;
  else if (
    (held && feature.reason === "agents-none") ||
    (personal
      ? feature.assistants.perUser === 0
      : feature.employees.allowed === 0)
  )
    row = 2;
  else if (held) row = 3;
  else if (personal ? !canWrite : !administrator) row = 4;
  else if (!personal && !canWrite) row = 5;
  else if (personal && mine >= perUser) row = 5;
  else if (
    personal
      ? feature.assistants.allowed - feature.assistants.inUse <= 0
      : feature.employees.inUse >= feature.employees.allowed
  )
    row = 6;
  else row = 7;
  const noButton = personal ? row === 4 : row === 4 || row === 5;
  const notice = (
    <ExecutionNotice
      area="agent"
      kind={kind}
      reason={feature.reason}
      administrator={administrator}
      provider={branding.providerName}
      supportURL={branding.supportURL}
      storageReason={storage?.reason}
    />
  );
  const sentence =
    row === 1
      ? t.sidebar.agentsUnavailable
      : row === 2
        ? strings.notOffered
        : row === 4
          ? personal
            ? copy.assistants.noPermission
            : copy.employees.adminsAdd
          : row === 5 && personal
            ? unfinishedName && !ownName
              ? copy.assistants.limitUnfinished(unfinishedName)
              : copy.assistants.limitReached(mine, perUser, ownName)
            : row === 6
              ? personal
                ? copy.assistants.noneFree(administrator, provider)
                : copy.employees.limitReached(
                    feature.employees.allowed,
                    provider,
                  )
              : null;

  useEffect(() => {
    if (creating || !pages || list.isFetching || !user) return;
    const remembered = readCreation(user.id);
    if (
      remembered &&
      (remembered.copy_of === null ? remembered.body.custody : "company") ===
        kind &&
      !unfinished.some((entry) => matchesCreation(remembered, entry))
    )
      clearCreation(user.id);
  }, [creating, pages, list.isFetching, user, unfinished, kind]);

  function refresh() {
    void client.invalidateQueries({ queryKey: ["features"] });
    void client.invalidateQueries({ queryKey: ["agent-instances"] });
  }
  function groups(rows: api.AgentListEntry[]) {
    return (["active", "suspended", "archived"] as const).map((status) => {
      const group = rows.filter((entry) => entry.status === status);
      if (!group.length) return null;
      return (
        <section key={status} className="space-y-3">
          <h2 className="text-lg font-semibold">
            {status === "active"
              ? copy.group.active
              : status === "suspended"
                ? copy.group.paused
                : copy.group.retired}
          </h2>
          <div className="grid gap-3 sm:grid-cols-2">
            {group.map((entry) => (
              <AgentCard
                key={entry.id}
                kind={kind}
                entry={entry}
                owner={
                  api.isFullInstance(entry) &&
                  personal &&
                  entry.owner_id !== user?.id
                }
              />
            ))}
          </div>
        </section>
      );
    });
  }
  return (
    <WorkspaceContainer>
      <WorkspaceHeader />
      <WorkspaceBody>
        <div className="mx-auto h-full w-full max-w-5xl space-y-5 overflow-y-auto p-6">
          <h1 className="text-2xl font-semibold">{strings.title}</h1>
          <p className="text-muted-foreground">{strings.description}</p>
          <DeletedAgentNotice kind={kind} />
          <div className="space-y-2">
            {!noButton && (
              <Button
                type="button"
                disabled={
                  row !== 7 || list.isPending || list.isError || !canRead
                }
                aria-describedby={sentence || row === 3 ? hintId : undefined}
                onClick={() => setCreating(true)}
              >
                {strings.add}
              </Button>
            )}
            {sentence && <p id={hintId}>{sentence}</p>}
            {row === 3 && <div id={hintId}>{notice}</div>}
            {row === 6 && administrator && (
              <ProviderSupportLink
                provider={branding.providerName}
                supportURL={branding.supportURL}
              />
            )}
          </div>
          {enabled && (
            <>
              {personal && (row === 5 || row === 7) && pages && (
                <p>
                  {copy.assistants.allowance(
                    mine,
                    perUser,
                    ownName.length ? ownName : (unfinishedName ?? ""),
                  )}
                </p>
              )}
              {!personal && administrator && (
                <p>
                  {copy.employees.allowance(
                    feature.employees.inUse,
                    feature.employees.allowed,
                  )}
                </p>
              )}
              {list.isPending ? (
                <p role="status">{copy.loading}</p>
              ) : list.isError ? (
                <div role="alert">
                  <p>{strings.listFailed}</p>
                  <Button type="button" variant="outline" onClick={refresh}>
                    {copy.refresh}
                  </Button>
                </div>
              ) : (
                <>
                  {!entries.length && !unfinished.length && (
                    <p>{strings.none}</p>
                  )}
                  {groups(personal ? own : entries)}
                  {!!unfinished.length && (
                    <section className="space-y-3">
                      <h2 className="text-lg font-semibold">
                        {copy.group.unfinished}
                      </h2>
                      {unfinished.map((entry) => (
                        <UnfinishedCard
                          key={entry.id}
                          entry={entry}
                          executable={feature.execution === "ready"}
                          notice={notice}
                          refresh={refresh}
                          providerName={branding.providerName}
                          supportURL={branding.supportURL}
                        />
                      ))}
                    </section>
                  )}
                  {!!others.length && (
                    <section className="space-y-4">
                      <h2 className="text-xl font-semibold">
                        {copy.assistants.othersHeading}
                      </h2>
                      {groups(others)}
                    </section>
                  )}
                  {list.hasNextPage && (
                    <Button
                      type="button"
                      variant="outline"
                      disabled={list.isFetchingNextPage}
                      onClick={() => void list.fetchNextPage()}
                    >
                      {copy.more}
                    </Button>
                  )}
                </>
              )}
            </>
          )}
          {personal ? (
            <CreateAssistant
              open={creating}
              onOpenChange={setCreating}
              providerName={branding.providerName}
              supportURL={branding.supportURL}
              ownName={ownName}
              unfinishedName={unfinishedName}
            />
          ) : (
            <CreateEmployee
              open={creating}
              onOpenChange={setCreating}
              providerName={branding.providerName}
              supportURL={branding.supportURL}
            />
          )}
        </div>
      </WorkspaceBody>
    </WorkspaceContainer>
  );
}

function AgentCard({
  entry,
  kind,
  owner,
}: {
  entry: api.AgentListEntry;
  kind: api.Custody;
  owner: boolean;
}) {
  return api.isFullInstance(entry) ? (
    <FullAgentCard entry={entry} owner={owner} />
  ) : (
    <SummaryAgentCard entry={entry} kind={kind} />
  );
}
function FullAgentCard({
  entry,
  owner,
}: {
  entry: api.AgentInstance;
  owner: boolean;
}) {
  const { t } = useI18n();
  const person = usePersonName(
    entry.custody === "company"
      ? entry.supervisor.subject_id
      : owner
        ? (entry.owner_id ?? "")
        : "",
    true,
  );
  return (
    <article className="flex items-start gap-3 rounded-lg border p-4">
      <AgentAvatar instance={entry} />
      <div>
        <Link
          href={`/workspace/instances?instance=${entry.id}`}
          className="font-medium underline"
        >
          {entry.name}
        </Link>
        {entry.custody === "company" ? (
          <p className="text-muted-foreground text-sm">
            {t.agentInstances.employees.supervisorLine(person ?? "…")}
          </p>
        ) : (
          owner && (
            <p className="text-muted-foreground text-sm">
              {t.agentInstances.assistants.ownerLine(person ?? "…")}
            </p>
          )
        )}
      </div>
    </article>
  );
}
function SummaryAgentCard({
  entry,
  kind,
}: {
  entry: api.AgentInstanceSummary;
  kind: api.Custody;
}) {
  const { t } = useI18n();
  return (
    <article className="flex items-start gap-3 rounded-lg border p-4">
      <AgentAvatar instance={{ ...entry, custody: kind }} />
      <div>
        <h3 className="font-medium">{entry.name}</h3>
        <p className="text-muted-foreground text-sm">
          {t.agentInstances.employees.supervisorLine(
            entry.supervisor_name?.trim().length
              ? entry.supervisor_name.trim()
              : t.people.aManager,
          )}
        </p>
      </div>
    </article>
  );
}
