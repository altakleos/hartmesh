"use client";

import { useQuery, useQueryClient } from "@tanstack/react-query";
import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import { useId, useRef, useState } from "react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { WorkPanel } from "@/components/workspace/instances/work-panel";
import {
  WorkspaceBody,
  WorkspaceContainer,
  WorkspaceHeader,
} from "@/components/workspace/workspace-container";
import * as api from "@/core/agent-instances/api";
import { useAgentsApiEnabled } from "@/core/agents";
import { listAgents } from "@/core/agents/api";
import { useAuth } from "@/core/auth/AuthProvider";
import { useDocumentTitle, useStorageSpacesEnabled } from "@/core/features";
import { useFileActionLifetime } from "@/core/file-areas/file-action-lifetime";
import { useI18n } from "@/core/i18n/hooks";
import { useSpaceActionSignal } from "@/core/spaces/lifetime";

const USE = 1,
  INSPECT = 2,
  MANAGE = 4;

export default function InstancesPage() {
  const { user } = useAuth();
  const { enabled: agentsEnabled, isLoading: agentsLoading } =
    useAgentsApiEnabled();
  const spaces = useStorageSpacesEnabled();
  const { t } = useI18n();
  const search = useSearchParams();
  useDocumentTitle(t.agentInstances.title, t.pages.appName);
  return (
    <WorkspaceContainer>
      <WorkspaceHeader />
      <WorkspaceBody>
        <div className="mx-auto flex h-full w-full max-w-5xl flex-col gap-4 overflow-y-auto p-6">
          <h1 className="text-2xl font-semibold">{t.agentInstances.title}</h1>
          <p className="text-muted-foreground">
            {t.agentInstances.description}
          </p>
          {agentsLoading || spaces.isLoading ? (
            <p role="status">{t.common.loading}</p>
          ) : !agentsEnabled || !spaces.enabled ? (
            <p role="status">{t.agentInstances.unavailable}</p>
          ) : (
            <InstanceControls
              key={`${user?.id}:${user?.system_role}:${user?.permissions?.join(",")}:${search.get("instance")}`}
            />
          )}
        </div>
      </WorkspaceBody>
    </WorkspaceContainer>
  );
}

function InstanceControls() {
  const { user } = useAuth();
  const { t } = useI18n();
  const copy = t.agentInstances;
  const queryClient = useQueryClient();
  const router = useRouter();
  const search = useSearchParams();
  const id = search.get("instance");
  const selected = id && /^[0-9a-f]{32}$/.test(id) ? id : null;
  const account = useFileActionLifetime();
  const captureSignal = useSpaceActionSignal();
  const [offset, setOffset] = useState(0);
  const [busy, setBusy] = useState(false);
  const busyRef = useRef(false);
  const [error, setError] = useState<string | null>(null);
  const [name, setName] = useState("");
  const [definition, setDefinition] = useState("");
  const [custody, setCustody] = useState<"personal" | "company">("personal");
  const [supervisor, setSupervisor] = useState("");
  const [member, setMember] = useState("");
  const [permissions, setPermissions] = useState(USE);
  const [fact, setFact] = useState("");
  const [importText, setImportText] = useState("");
  const [accept, setAccept] = useState(false);
  const [creation, setCreation] = useState<api.CreateInstance | null>(null);
  const [companyCopy, setCompanyCopy] = useState<{
    creation_id: string;
    generation: number;
    name: string;
    supervisor_id?: string;
  } | null>(null);
  const [pending, setPending] = useState<api.LifecycleChange | null>(null);
  const formId = useId();
  const ceiling = (permission: string) =>
    !!user &&
    (user.permissions == null ||
      user.permissions.includes(permission) ||
      user.permissions.includes("*") ||
      user.permissions.includes(permission.split(":")[0] + ":*"));
  const key = ["agent-instances", user?.id, user?.system_role];
  const instances = useQuery({
    queryKey: [...key, "list", offset],
    queryFn: ({ signal }) => api.listInstances(offset, signal),
    enabled: ceiling("agents:read"),
    staleTime: 0,
  });
  const instance = useQuery({
    queryKey: [...key, selected],
    queryFn: ({ signal }) => api.getInstance(selected!, signal),
    enabled: selected !== null && ceiling("agents:read"),
    staleTime: 0,
  });
  const current = instance.data;
  const canInspect = !!current && (current.permissions & INSPECT) === INSPECT;
  const canManage =
    !!current &&
    (current.permissions & MANAGE) === MANAGE &&
    ceiling("agents:write");
  const definitions = useQuery({
    queryKey: [...key, "definitions"],
    queryFn: listAgents,
    enabled: ceiling("agents:write") && (!selected || canManage),
  });
  const adopted = useQuery({
    queryKey: [...key, selected, "definition"],
    queryFn: ({ signal }) => api.getInstanceDefinition(selected!, signal),
    enabled: canInspect,
    staleTime: 0,
  });
  const grants = useQuery({
    queryKey: [...key, selected, "grants"],
    queryFn: ({ signal }) => api.getInstanceGrants(selected!, signal),
    enabled: canManage,
    staleTime: 0,
  });
  const lifecycle = useQuery({
    queryKey: [...key, selected, "lifecycle"],
    queryFn: ({ signal }) => api.getInstanceLifecycle(selected!, signal),
    enabled: canInspect || canManage,
    staleTime: 0,
  });
  const memory = useQuery({
    queryKey: [...key, selected, "memory"],
    queryFn: ({ signal }) => api.getInstanceMemory(selected!, signal),
    enabled:
      canInspect && current?.status !== "deleted" && ceiling("memory:read"),
    staleTime: 0,
    retry: false,
  });
  const unresolved = lifecycle.data?.operations.find(
    (operation) => !operation.complete,
  );
  const memoryWrite =
    canManage &&
    canInspect &&
    current?.status === "active" &&
    ceiling("memory:read") &&
    ceiling("memory:write");

  async function run(work: (signal: AbortSignal) => Promise<void>) {
    if (busyRef.current) return;
    busyRef.current = true;
    const signal = captureSignal();
    setBusy(true);
    setError(null);
    try {
      await work(signal);
    } catch (failure) {
      if (!signal.aborted && account.active)
        setError(
          failure instanceof Error ? failure.message : copy.operationError,
        );
    } finally {
      busyRef.current = false;
      if (!signal.aborted && account.active) setBusy(false);
    }
  }
  async function refresh(signal?: AbortSignal) {
    if (signal?.aborted || !account.active) return;
    await queryClient.invalidateQueries({ queryKey: key });
  }
  function change(action: api.LifecycleChange["action"]) {
    if (!current || !canManage || (unresolved ?? pending)) return;
    const body: api.LifecycleChange = {
      generation: current.generation,
      operation_id: api.operationID(),
      action,
      ...(action === "adopt" ? { definition_name: definition } : {}),
      ...(action === "supervise" ? { supervisor_id: supervisor } : {}),
      ...(action === "grant" ? { member_id: member, permissions } : {}),
    };
    setPending(body);
    void execute(body);
  }
  async function execute(body: api.LifecycleChange) {
    if (!current || !canManage) return;
    await run(async (signal) => {
      const result = await api.changeInstanceLifecycle(
        current.id,
        body,
        signal,
      );
      if (signal.aborted || !account.active) return;
      setPending(result.complete ? null : body);
      setAccept(false);
      await refresh(signal);
    });
  }
  function field(
    label: string,
    value: string,
    update: (value: string) => void,
    name: string,
  ) {
    return (
      <label className="flex flex-col gap-1" htmlFor={`${formId}-${name}`}>
        <span>{label}</span>
        <Input
          id={`${formId}-${name}`}
          value={value}
          onChange={(event) => update(event.target.value)}
          disabled={busy}
        />
      </label>
    );
  }
  const definitionPicker = (
    <label className="flex flex-col gap-1" htmlFor={`${formId}-definition`}>
      <span>{copy.definition}</span>
      <select
        id={`${formId}-definition`}
        className="bg-background rounded border p-2"
        value={definition}
        onChange={(event) => setDefinition(event.target.value)}
        disabled={busy}
      >
        <option value="">—</option>
        {definitions.data?.map((value) => (
          <option key={value.name} value={value.name}>
            {value.name}
          </option>
        ))}
      </select>
    </label>
  );

  return (
    <>
      {error && <p role="alert">{error}</p>}
      {instances.error && <p role="alert">{instances.error.message}</p>}
      <div className="flex flex-wrap items-center gap-2">
        <Button
          variant="outline"
          disabled={busy}
          onClick={() => void run((signal) => refresh(signal))}
        >
          {copy.refresh}
        </Button>
        <Link href="/workspace/spaces">{copy.spaces}</Link>
      </div>
      <ul className="grid gap-2 sm:grid-cols-2">
        {instances.data?.instances.map((value) => (
          <li key={value.id} className="rounded border p-3">
            <Link href={`/workspace/instances?instance=${value.id}`}>
              {value.name}
            </Link>
            <p className="text-muted-foreground text-sm">
              {copy[value.custody]} · {copy[value.status]}
            </p>
          </li>
        ))}
      </ul>
      {instances.data?.instances.length === 0 && <p>{copy.none}</p>}
      {offset > 0 && (
        <Button
          variant="outline"
          onClick={() => setOffset(Math.max(0, offset - 100))}
        >
          ←
        </Button>
      )}
      {instances.data?.instances.length === 100 && (
        <Button variant="outline" onClick={() => setOffset(offset + 100)}>
          {copy.more}
        </Button>
      )}
      {!selected && ceiling("agents:write") && (
        <section className="grid gap-3 rounded border p-4 sm:grid-cols-2">
          {field(copy.name, name, setName, "create-name")}
          {definitionPicker}
          <label className="flex flex-col gap-1" htmlFor={`${formId}-custody`}>
            <span>{copy.custody}</span>
            <select
              id={`${formId}-custody`}
              className="bg-background rounded border p-2"
              value={custody}
              onChange={(event) =>
                setCustody(event.target.value as "personal" | "company")
              }
            >
              <option value="personal">{copy.personal}</option>
              <option value="company">{copy.company}</option>
            </select>
          </label>
          {field(
            copy.supervisor,
            supervisor,
            setSupervisor,
            "create-supervisor",
          )}
          <Button
            disabled={busy || !name.trim() || !definition}
            onClick={() =>
              void run(async (signal) => {
                const body = creation ?? {
                  creation_id: api.operationID(),
                  name,
                  definition_name: definition,
                  custody,
                  ...(supervisor ? { supervisor_id: supervisor } : {}),
                };
                setCreation(body);
                const created = await api.createInstance(body, signal);
                if (signal.aborted || !account.active) return;
                setCreation(null);
                await refresh(signal);
                if (!signal.aborted && account.active)
                  router.replace(`/workspace/instances?instance=${created.id}`);
              })
            }
          >
            {copy.create}
          </Button>
        </section>
      )}
      {selected &&
        (instance.isPending ? (
          <p role="status">{copy.loading}</p>
        ) : instance.error ? (
          <p role="alert">{instance.error.message}</p>
        ) : (
          current && (
            <section
              key={current.id}
              className="flex flex-col gap-4 rounded border p-4"
            >
              <h2 className="text-xl font-semibold">{current.name}</h2>
              <dl className="grid gap-2 sm:grid-cols-2">
                <div>
                  <dt>{copy.custody}</dt>
                  <dd>{copy[current.custody]}</dd>
                </div>
                <div>
                  <dt>{copy.status}</dt>
                  <dd>{copy[current.status]}</dd>
                </div>
                <div>
                  <dt>{copy.supervisor}</dt>
                  <dd>{current.supervisor.subject_id}</dd>
                </div>
                <div>
                  <dt>{copy.definition}</dt>
                  <dd>
                    {adopted.data?.config.name ??
                      current.definition_revision.slice(0, 12)}
                  </dd>
                </div>
              </dl>
              <p>
                {[USE, INSPECT, MANAGE]
                  .filter((bit) => (current.permissions & bit) !== 0)
                  .map((bit) =>
                    bit === USE
                      ? copy.use
                      : bit === INSPECT
                        ? copy.inspect
                        : copy.manageAccess,
                  )
                  .join(" · ")}
              </p>
              {current.home_id && (
                <Link href={`/workspace/spaces?space=${current.home_id}`}>
                  {copy.home}
                </Link>
              )}
              {!canInspect && <p>{copy.useOnly}</p>}
              <WorkPanel
                instance={current}
                policy={adopted.error ? null : adopted.data?.config.work_policy}
                canRead={ceiling("agents:read")}
                canWrite={ceiling("agents:write")}
                canActivate={ceiling("runs:create")}
                canCreateConversation={ceiling("threads:write")}
                canStop={ceiling("runs:cancel")}
                initialWork={search.get("work")}
                lifecycleOperations={lifecycle.data?.operations}
                lifecyclePending={!!(unresolved ?? pending)}
              />
              {(current.permissions & USE) !== 0 &&
                current.status === "active" &&
                ceiling("threads:write") && (
                  <Button
                    disabled={busy || !!(unresolved ?? pending)}
                    onClick={() =>
                      void run(async (signal) => {
                        const chat = await api.createInstanceConversation(
                          current.id,
                          api.operationID(),
                          signal,
                        );
                        if (!signal.aborted && account.active)
                          router.push(`/workspace/chats/${chat.thread_id}`);
                      })
                    }
                  >
                    {copy.start}
                  </Button>
                )}
              {adopted.data && (
                <details>
                  <summary>{copy.instructions}</summary>
                  <p className="whitespace-pre-wrap">{adopted.data.soul}</p>
                </details>
              )}
              {(unresolved ?? pending) && (
                <div role="status">
                  <p>{unresolved ? copy.pending : copy.unconfirmed}</p>
                  {canManage && (pending ?? unresolved?.retry) && (
                    <Button
                      disabled={busy}
                      onClick={() =>
                        void execute(pending ?? unresolved!.retry!)
                      }
                    >
                      {copy.retry}
                    </Button>
                  )}
                  {canManage && unresolved && (
                    <Button
                      variant="outline"
                      disabled={busy || !accept}
                      onClick={() =>
                        void run(async (signal) => {
                          await api.abandonInstanceLifecycle(
                            current.id,
                            {
                              generation: current.generation,
                              operation_id: unresolved.operation_id,
                            },
                            signal,
                          );
                          if (signal.aborted || !account.active) return;
                          setPending(null);
                          setAccept(false);
                          await refresh(signal);
                        })
                      }
                    >
                      {copy.abandon}
                    </Button>
                  )}
                </div>
              )}
              {lifecycle.data?.operations.some(
                (operation) => operation.abandoned,
              ) && <p role="status">{copy.abandoned}</p>}
              {canManage && (
                <section className="flex flex-col gap-3 border-t pt-4">
                  <h3 className="font-semibold">{copy.manage}</h3>
                  <p>{copy.retention}</p>
                  {canInspect &&
                    current.custody === "personal" &&
                    current.owner_id === user?.id &&
                    current.status !== "deleted" && (
                      <>
                        <p>{copy.companyCopyNotice}</p>
                        <Button
                          variant="outline"
                          disabled={
                            busy || !accept || !!(unresolved ?? pending)
                          }
                          onClick={() =>
                            void run(async (signal) => {
                              const body = companyCopy ?? {
                                creation_id: api.operationID(),
                                generation: current.generation,
                                name:
                                  name.trim() ||
                                  `${current.name.slice(0, 110)} (company)`,
                                ...(supervisor
                                  ? { supervisor_id: supervisor }
                                  : {}),
                              };
                              setCompanyCopy(body);
                              const target = await api.createCompanyCopy(
                                current.id,
                                body,
                                signal,
                              );
                              if (signal.aborted || !account.active) return;
                              setCompanyCopy(null);
                              await refresh(signal);
                              if (!signal.aborted && account.active)
                                router.replace(
                                  `/workspace/instances?instance=${target.id}`,
                                );
                            })
                          }
                        >
                          {copy.companyCopy}
                        </Button>
                      </>
                    )}
                  {field(copy.name, name, setName, "rename")}
                  <Button
                    variant="outline"
                    disabled={
                      busy ||
                      !name.trim() ||
                      !!(unresolved ?? pending) ||
                      current.status === "deleted"
                    }
                    onClick={() =>
                      void run(async (signal) => {
                        await api.renameInstance(
                          current.id,
                          current.generation,
                          name,
                          signal,
                        );
                        await refresh(signal);
                      })
                    }
                  >
                    {copy.rename}
                  </Button>
                  <label>
                    <input
                      type="checkbox"
                      checked={accept}
                      onChange={(event) => setAccept(event.target.checked)}
                    />{" "}
                    {copy.confirm}
                  </label>
                  <div className="flex flex-wrap gap-2">
                    {(["suspend", "archive", "delete", "restore"] as const).map(
                      (action) => (
                        <Button
                          key={action}
                          variant="outline"
                          disabled={
                            busy ||
                            !accept ||
                            !!(unresolved ?? pending) ||
                            (action === "restore"
                              ? current.status === "active"
                              : current.status === "deleted")
                          }
                          onClick={() => change(action)}
                        >
                          {copy[action]}
                        </Button>
                      ),
                    )}
                  </div>
                  {current.status !== "deleted" && (
                    <>
                      {definitionPicker}
                      <Button
                        variant="outline"
                        disabled={
                          busy || !definition || !!(unresolved ?? pending)
                        }
                        onClick={() => change("adopt")}
                      >
                        {copy.adopt}
                      </Button>
                      {field(
                        copy.supervisor,
                        supervisor,
                        setSupervisor,
                        "supervise",
                      )}
                      <Button
                        variant="outline"
                        disabled={
                          busy || !supervisor || !!(unresolved ?? pending)
                        }
                        onClick={() => change("supervise")}
                      >
                        {copy.supervise}
                      </Button>
                      <p>{copy.grantsNotice}</p>
                      <ul>
                        {grants.data?.grants.map((value) => (
                          <li key={value.user_id}>
                            {value.user_id} ·{" "}
                            {[USE, INSPECT, MANAGE]
                              .filter((bit) => (value.permissions & bit) !== 0)
                              .map((bit) =>
                                bit === USE
                                  ? copy.use
                                  : bit === INSPECT
                                    ? copy.inspect
                                    : copy.manageAccess,
                              )
                              .join(" · ")}
                          </li>
                        ))}
                      </ul>
                      {field(copy.member, member, setMember, "grant-member")}
                      <div className="flex flex-wrap gap-3">
                        {([USE, INSPECT, MANAGE] as const).map((bit) => (
                          <label key={bit}>
                            <input
                              type="checkbox"
                              checked={(permissions & bit) !== 0}
                              onChange={(event) =>
                                setPermissions(
                                  event.target.checked
                                    ? permissions | bit
                                    : permissions & ~bit,
                                )
                              }
                            />
                            {bit === USE
                              ? copy.use
                              : bit === INSPECT
                                ? copy.inspect
                                : copy.manageAccess}
                          </label>
                        ))}
                      </div>
                      <Button
                        variant="outline"
                        disabled={busy || !member || !!(unresolved ?? pending)}
                        onClick={() => change("grant")}
                      >
                        {copy.grant}
                      </Button>
                    </>
                  )}
                </section>
              )}
              {canInspect &&
                ceiling("memory:read") &&
                current.status !== "deleted" && (
                  <section className="flex flex-col gap-3 border-t pt-4">
                    <h3 className="font-semibold">{copy.memory}</h3>
                    <p>{copy.memoryNotice}</p>
                    {memory.error && (
                      <p role="status">
                        {copy.memoryUnavailable} {memory.error.message}
                      </p>
                    )}
                    {memory.data && (
                      <>
                        <dl className="grid gap-3 sm:grid-cols-2">
                          {[
                            [
                              t.settings.memory.markdown.work,
                              memory.data.user.workContext.summary,
                            ],
                            [
                              t.settings.memory.markdown.personal,
                              memory.data.user.personalContext.summary,
                            ],
                            [
                              t.settings.memory.markdown.topOfMind,
                              memory.data.user.topOfMind.summary,
                            ],
                            [
                              t.settings.memory.markdown.recentMonths,
                              memory.data.history.recentMonths.summary,
                            ],
                            [
                              t.settings.memory.markdown.earlierContext,
                              memory.data.history.earlierContext.summary,
                            ],
                            [
                              t.settings.memory.markdown.longTermBackground,
                              memory.data.history.longTermBackground.summary,
                            ],
                          ]
                            .filter(([, summary]) => summary)
                            .map(([label, summary]) => (
                              <div key={label}>
                                <dt className="font-semibold">{label}</dt>
                                <dd className="whitespace-pre-wrap">
                                  {summary}
                                </dd>
                              </div>
                            ))}
                        </dl>
                        <ul>
                          {memory.data.facts.map((value) => (
                            <li
                              key={value.id}
                              className="flex items-start justify-between gap-3 rounded border p-2"
                            >
                              <p className="whitespace-pre-wrap">
                                {value.content}
                              </p>
                              {memoryWrite && (
                                <Button
                                  variant="outline"
                                  disabled={busy}
                                  onClick={() =>
                                    void run(async (signal) => {
                                      await api.deleteInstanceFact(
                                        current.id,
                                        value.id,
                                        signal,
                                      );
                                      await refresh(signal);
                                    })
                                  }
                                >
                                  {copy.removeFact}
                                </Button>
                              )}
                            </li>
                          ))}
                        </ul>
                        <Button
                          variant="outline"
                          onClick={() => {
                            const blob = new Blob(
                              [JSON.stringify(memory.data, null, 2)],
                              { type: "application/json" },
                            );
                            const url = URL.createObjectURL(blob);
                            const anchor = document.createElement("a");
                            anchor.href = url;
                            anchor.download = "instance-memory.json";
                            anchor.click();
                            URL.revokeObjectURL(url);
                          }}
                        >
                          {copy.exportMemory}
                        </Button>
                        {memoryWrite && (
                          <>
                            {field(copy.fact, fact, setFact, "memory-fact")}
                            <Button
                              disabled={busy || !fact.trim()}
                              onClick={() =>
                                void run(async (signal) => {
                                  await api.createInstanceFact(
                                    current.id,
                                    fact,
                                    signal,
                                  );
                                  if (!signal.aborted) setFact("");
                                  await refresh(signal);
                                })
                              }
                            >
                              {copy.addFact}
                            </Button>
                            <p>{copy.confirmMemory}</p>
                            <Button
                              variant="outline"
                              disabled={busy || !accept}
                              onClick={() =>
                                void run(async (signal) => {
                                  await api.clearInstanceMemory(
                                    current.id,
                                    signal,
                                  );
                                  await refresh(signal);
                                })
                              }
                            >
                              {copy.clearMemory}
                            </Button>
                            <label htmlFor={`${formId}-memory-import`}>
                              {copy.importMemory}
                            </label>
                            <textarea
                              id={`${formId}-memory-import`}
                              className="min-h-24 rounded border p-2"
                              value={importText}
                              onChange={(event) =>
                                setImportText(event.target.value)
                              }
                            />
                            <Button
                              variant="outline"
                              disabled={busy || !accept || !importText.trim()}
                              onClick={() =>
                                void run(async (signal) => {
                                  if (
                                    new TextEncoder().encode(importText)
                                      .length >
                                    4 * 1024 * 1024
                                  )
                                    throw new Error(
                                      "Memory import exceeds4MiB",
                                    );
                                  const document: unknown =
                                    JSON.parse(importText);
                                  await api.importInstanceMemory(
                                    current.id,
                                    document,
                                    signal,
                                  );
                                  await refresh(signal);
                                })
                              }
                            >
                              {copy.replaceMemory}
                            </Button>
                          </>
                        )}
                      </>
                    )}
                  </section>
                )}
            </section>
          )
        ))}
    </>
  );
}
