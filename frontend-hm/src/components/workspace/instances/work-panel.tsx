"use client";

import { useQuery, useQueryClient } from "@tanstack/react-query";
import Link from "next/link";
import { useRef, useState } from "react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { RequestInput } from "@/components/workspace/attention/create";
import * as api from "@/core/agent-instances/api";
import type { WorkPolicy } from "@/core/agents/types";
import { useAuth } from "@/core/auth/AuthProvider";
import { useFileActionLifetime } from "@/core/file-areas/file-action-lifetime";
import { useI18n } from "@/core/i18n/hooks";
import { listSpaces } from "@/core/spaces/api";
import { useSpaceActionSignal } from "@/core/spaces/lifetime";

interface Props {
  instance: api.AgentInstance;
  policy?: WorkPolicy | null;
  canRead: boolean;
  canWrite: boolean;
  lifecyclePending: boolean;
}
type Pending =
  | { kind: "delegate"; body: api.WorkAssignment & { operation_id: string } }
  | { kind: "command"; work: string; body: api.WorkCommand };

export function WorkPanel(props: Props) {
  const { user } = useAuth();
  // Identity and permission changes retire every draft, query and pending action.
  return (
    <WorkControls
      key={`${user?.id}:${user?.system_role}:${user?.permissions?.join(",")}:${props.instance.id}:${props.instance.permissions}:${props.canRead}:${props.canWrite}`}
      {...props}
    />
  );
}

function WorkControls({
  instance,
  policy,
  canRead,
  canWrite,
  lifecyclePending,
}: Props) {
  const { user } = useAuth();
  const { t } = useI18n();
  const copy = t.agentWork;
  const client = useQueryClient();
  const account = useFileActionLifetime();
  const captureSignal = useSpaceActionSignal();
  const inspect = canRead && (instance.permissions & 2) !== 0;
  const manage = inspect && canWrite && (instance.permissions & 4) !== 0;
  const collaborate = inspect && canWrite && (instance.permissions & 1) !== 0;
  const [offset, setOffset] = useState(0);
  const [selected, setSelected] = useState<string | null>(null);
  const [pending, setPending] = useState<Pending | null>(null);
  const [busy, setBusy] = useState(false);
  const busyRef = useRef(false);
  const [error, setError] = useState<string | null>(null);
  const [formVersion, setFormVersion] = useState(0);
  const key = [
    "agent-work",
    user?.id,
    user?.system_role,
    user?.permissions,
    instance.id,
    instance.permissions,
  ];
  const records = useQuery({
    queryKey: [...key, "list", offset],
    queryFn: ({ signal }) => api.listWork(instance.id, offset, signal),
    enabled: inspect,
    retry: false,
    staleTime: 0,
    gcTime: 0,
  });
  const detail = useQuery({
    queryKey: [...key, "record", selected],
    queryFn: ({ signal }) => api.getWork(instance.id, selected!, signal),
    enabled: inspect && !!selected,
    retry: false,
    staleTime: 0,
    gcTime: 0,
  });
  async function execute(intent: Pending) {
    if (busyRef.current || !account.active) return;
    busyRef.current = true;
    const signal = captureSignal();
    setBusy(true);
    setPending(intent);
    setError(null);
    try {
      const result =
        intent.kind === "delegate"
          ? await api.delegateWork(instance.id, intent.body, signal)
          : await api.commandWork(
              instance.id,
              intent.work,
              intent.body,
              signal,
            );
      if (signal.aborted || !account.active) return;
      setPending(null);
      setSelected(result.id);
      setFormVersion((value) => value + 1);
      await client.invalidateQueries({ queryKey: key });
    } catch (failure) {
      if (signal.aborted || !account.active) return;
      // A definite rejection permits correcting the draft. Unknown transport
      // outcomes retain the exact body/ID; never silently duplicate delegation.
      const definite =
        failure instanceof api.InstanceApiError &&
        failure.status >= 400 &&
        failure.status < 500;
      if (definite) setPending(null);
      setError(definite ? failure.message : copy.unconfirmed);
      if (definite) await client.invalidateQueries({ queryKey: key });
    } finally {
      busyRef.current = false;
      if (!signal.aborted && account.active) setBusy(false);
    }
  }
  const disabled = busy || pending !== null;
  if (!inspect)
    return (
      <section>
        <h3>{copy.title}</h3>
        <p>{copy.visibility}</p>
      </section>
    );
  return (
    <section
      className="flex flex-col gap-3 border-t pt-4"
      aria-label={copy.title}
    >
      <h3 className="font-semibold">{copy.title}</h3>
      <p role="status">{copy.recordsOnly}</p>
      <p className="text-muted-foreground text-sm">{copy.sharedNotice}</p>
      {policy?.responsibilities?.length ? (
        <ul>
          {policy.responsibilities.map((item) => (
            <li key={item.key}>{item.label}</li>
          ))}
        </ul>
      ) : null}
      {!policy?.enabled && <p>{copy.policyDisabled}</p>}
      {error && <p role="alert">{error}</p>}
      {pending && (
        <Button disabled={busy} onClick={() => void execute(pending)}>
          {copy.retry}
        </Button>
      )}
      <Button
        variant="outline"
        disabled={busy}
        onClick={() => void client.invalidateQueries({ queryKey: key })}
      >
        {copy.refresh}
      </Button>
      {collaborate &&
        policy?.enabled &&
        instance.status === "active" &&
        !lifecyclePending && (
          <AssignmentForm
            key={formVersion}
            policy={policy}
            manager={manage}
            disabled={disabled}
            submit={(assignment) =>
              void execute({
                kind: "delegate",
                body: { ...assignment, operation_id: api.operationID() },
              })
            }
          />
        )}
      {!collaborate && <p>{copy.visibility}</p>}
      {records.isPending ? (
        <p>{t.common.loading}</p>
      ) : records.error ? (
        <p role="alert">{records.error.message}</p>
      ) : (
        <>
          {!records.data?.work.length && <p>{copy.empty}</p>}
          <ul className="space-y-2">
            {records.data?.work.map((record) => (
              <li key={record.id}>
                <Button
                  variant="outline"
                  className="h-auto w-full justify-start text-left whitespace-normal"
                  onClick={() => setSelected(record.id)}
                >
                  {record.objective} · {copy[record.status]} ·{" "}
                  {copy[record.priority ?? "normal"]}
                </Button>
              </li>
            ))}
          </ul>
          <div className="flex gap-2">
            <Button
              variant="outline"
              disabled={offset === 0}
              onClick={() => setOffset((value) => Math.max(0, value - 50))}
            >
              {copy.previous}
            </Button>
            <Button
              variant="outline"
              disabled={records.data?.work.length !== 50}
              onClick={() => setOffset((value) => value + 50)}
            >
              {copy.next}
            </Button>
          </div>
        </>
      )}
      {selected &&
        (detail.isPending ? (
          <p>{t.common.loading}</p>
        ) : detail.error ? (
          <p role="alert">{detail.error.message}</p>
        ) : (
          detail.data && (
            <div>
              <RequestInput
                key={`request:${selected}:${formVersion}`}
                record={detail.data}
                manager={manage}
                disabled={disabled || lifecyclePending}
              />
              <WorkDetail
                key={`${selected}:${formVersion}`}
                record={detail.data}
                policy={policy}
                manager={manage}
                collaborate={collaborate}
                disabled={disabled}
                queryKey={key}
                submit={(body) =>
                  void execute({ kind: "command", work: selected, body })
                }
              />
            </div>
          )
        ))}
    </section>
  );
}

function AssignmentForm({
  record,
  policy,
  manager,
  disabled,
  submit,
  cancel,
}: {
  record?: api.WorkRecord;
  policy?: WorkPolicy | null;
  manager: boolean;
  disabled: boolean;
  submit: (assignment: api.WorkAssignment) => void;
  cancel?: () => void;
}) {
  const { t } = useI18n();
  const copy = t.agentWork;
  const { user } = useAuth();
  const [objective, setObjective] = useState(record?.objective ?? "");
  const [criteria, setCriteria] = useState(record?.success_criteria ?? "");
  const [responsibility, setResponsibility] = useState(
    record?.responsibility ?? "",
  );
  const [priority, setPriority] = useState<api.WorkPriority>(
    record?.priority ?? policy?.default_priority ?? "normal",
  );
  const [review, setReview] = useState(
    record?.review_required ?? policy?.review_required ?? true,
  );
  const localDate = (value: string) => {
    const date = new Date(value);
    return new Date(date.getTime() - date.getTimezoneOffset() * 60_000)
      .toISOString()
      .slice(0, 16);
  };
  const [due, setDue] = useState(
    record?.due_at ? localDate(record.due_at) : "",
  );
  const [sources, setSources] = useState<api.WorkSource[]>(
    record?.sources
      .filter(
        (source): source is api.WorkSource => source.kind === "space_file",
      )
      .map(({ kind, space_id, path }) => ({ kind, space_id, path })) ?? [],
  );
  const hasUnavailable = !!record?.sources.some(
    (source) => source.kind === "unavailable",
  );
  const [replaceUnavailable, setReplaceUnavailable] = useState(false);
  const [sourcesChanged, setSourcesChanged] = useState(false);
  const [space, setSpace] = useState("");
  const [path, setPath] = useState("");
  const spaces = useQuery({
    queryKey: [
      "work-source-spaces",
      user?.id,
      user?.system_role,
      user?.permissions,
    ],
    queryFn: ({ signal }) => listSpaces(signal),
    retry: false,
    gcTime: 0,
  });
  return (
    <form
      className="flex flex-col gap-3 rounded border p-3"
      onSubmit={(event) => {
        event.preventDefault();
        if (
          disabled ||
          (sourcesChanged && hasUnavailable && !replaceUnavailable)
        )
          return;
        submit({
          objective: objective.trim(),
          success_criteria: criteria.trim(),
          responsibility: responsibility || null,
          ...(manager
            ? {
                priority,
                due_at: due ? new Date(due).toISOString() : null,
                review_required: review,
              }
            : {}),
          ...(!record || sourcesChanged ? { sources } : {}),
        });
      }}
    >
      <fieldset disabled={disabled} className="flex flex-col gap-3">
        <label>
          {copy.objective}
          <Input
            required
            maxLength={4096}
            value={objective}
            onChange={(event) => setObjective(event.target.value)}
          />
        </label>
        <label>
          {copy.criteria}
          <textarea
            className="w-full rounded border p-2"
            required
            maxLength={4096}
            value={criteria}
            onChange={(event) => setCriteria(event.target.value)}
          />
        </label>
        {!!policy?.responsibilities?.length && (
          <label>
            {copy.responsibility}
            <select
              value={responsibility}
              onChange={(event) => setResponsibility(event.target.value)}
            >
              <option value="">—</option>
              {policy.responsibilities.map((item) => (
                <option key={item.key} value={item.key}>
                  {item.label}
                </option>
              ))}
            </select>
          </label>
        )}
        {manager && (
          <>
            <label>
              {copy.priority}
              <select
                value={priority}
                onChange={(event) =>
                  setPriority(event.target.value as api.WorkPriority)
                }
              >
                {(["low", "normal", "high", "urgent"] as const).map((value) => (
                  <option key={value} value={value}>
                    {copy[value]}
                  </option>
                ))}
              </select>
            </label>
            <label>
              {copy.due}
              <Input
                type="datetime-local"
                value={due}
                onChange={(event) => setDue(event.target.value)}
              />
            </label>
            <label className="flex items-center gap-2">
              <input
                type="checkbox"
                checked={review}
                disabled={policy?.review_required !== false}
                onChange={(event) => setReview(event.target.checked)}
              />
              {copy.reviewRequired}
            </label>
          </>
        )}
        <details>
          <summary>{copy.sources}</summary>
          <p className="text-sm">{copy.unchecked}</p>
          {record?.sources.some((source) => source.kind === "unavailable") && (
            <label>
              <input
                type="checkbox"
                checked={replaceUnavailable}
                onChange={(event) =>
                  setReplaceUnavailable(event.target.checked)
                }
              />
              {copy.replaceUnavailable}
            </label>
          )}
          <ul>
            {sources.map((source, index) => (
              <li key={`${source.space_id}:${source.path}:${index}`}>
                {source.path}
                <Button
                  type="button"
                  variant="ghost"
                  disabled={hasUnavailable && !replaceUnavailable}
                  onClick={() => {
                    setSources(sources.filter((_, i) => i !== index));
                    setSourcesChanged(true);
                  }}
                >
                  {copy.removeSource}
                </Button>
              </li>
            ))}
          </ul>
          {spaces.error && <p role="status">{copy.unavailableSource}</p>}
          <label>
            {copy.space}
            <select
              value={space}
              onChange={(event) => setSpace(event.target.value)}
            >
              <option value="">—</option>
              {spaces.data?.spaces.map((item) => (
                <option key={item.id} value={item.id}>
                  {item.name}
                </option>
              ))}
            </select>
          </label>
          <label>
            {copy.path}
            <Input
              value={path}
              maxLength={1024}
              onChange={(event) => setPath(event.target.value)}
            />
          </label>
          <Button
            type="button"
            variant="outline"
            disabled={
              !space ||
              !path ||
              sources.length >= 16 ||
              (hasUnavailable && !replaceUnavailable)
            }
            onClick={() => {
              setSources([
                ...sources,
                { kind: "space_file", space_id: space, path },
              ]);
              setSourcesChanged(true);
              setPath("");
            }}
          >
            {copy.addSource}
          </Button>
        </details>
        <div className="flex gap-2">
          <Button
            type="submit"
            disabled={
              !objective.trim() ||
              !criteria.trim() ||
              (sourcesChanged && hasUnavailable && !replaceUnavailable)
            }
          >
            {record ? copy.save : copy.delegate}
          </Button>
          {cancel && (
            <Button type="button" variant="outline" onClick={cancel}>
              {copy.cancelEdit}
            </Button>
          )}
        </div>
      </fieldset>
    </form>
  );
}

function Sources({ sources }: { sources: api.VisibleWorkSource[] }) {
  const { t } = useI18n();
  return (
    <ul>
      {sources.map((source, index) => (
        <li key={index}>
          {source.kind === "space_file" ? (
            <Link href={`/workspace/spaces?space=${source.space_id}`}>
              {source.path}
            </Link>
          ) : (
            t.agentWork.unavailableSource
          )}
        </li>
      ))}
    </ul>
  );
}

function WorkDetail({
  record,
  policy,
  manager,
  collaborate,
  disabled,
  queryKey,
  submit,
}: {
  record: api.WorkRecord;
  policy?: WorkPolicy | null;
  manager: boolean;
  collaborate: boolean;
  disabled: boolean;
  queryKey: unknown[];
  submit: (body: api.WorkCommand) => void;
}) {
  const { t } = useI18n();
  const copy = t.agentWork;
  const [editing, setEditing] = useState<api.WorkRecord | null>(null);
  const [note, setNote] = useState("");
  const [acceptanceBasis, setAcceptanceBasis] = useState<api.WorkRecord | null>(
    null,
  );
  const [noteBasis, setNoteBasis] = useState<api.WorkRecord | null>(null);
  const [historyOpen, setHistoryOpen] = useState(false);
  const [offset, setOffset] = useState(0);
  const history = useQuery({
    queryKey: [...queryKey, "history", record.id, offset],
    queryFn: ({ signal }) =>
      api.workHistory(record.instance_id, record.id, offset, signal),
    enabled: historyOpen,
    retry: false,
    staleTime: 0,
    gcTime: 0,
  });
  function command(
    action: api.WorkCommand["action"],
    extra: Partial<api.WorkCommand> = {},
    basis = noteBasis ?? record,
  ) {
    submit({
      operation_id: api.operationID(),
      expected_revision: basis.revision,
      expected_assignment_revision: basis.assignment_revision,
      action,
      ...(note.trim() ? { note: note.trim() } : {}),
      ...extra,
    });
  }
  const editable = record.status === "open" || record.status === "blocked";
  return (
    <article className="flex flex-col gap-3 rounded border p-4">
      <h4 className="font-semibold">{record.objective}</h4>
      <p>
        {copy[record.status]} · {copy[record.priority ?? "normal"]}
      </p>
      <p className="whitespace-pre-wrap">{record.success_criteria}</p>
      <p>
        {copy.creator}: {record.creator_id}
      </p>
      <p>
        {copy.lastActivity}: {new Date(record.updated_at).toLocaleString()}
      </p>
      {record.due_at && (
        <p>
          {copy.due}: {new Date(record.due_at).toLocaleString()}
        </p>
      )}
      {record.progress && (
        <p>
          {copy.progress}: {record.progress}
        </p>
      )}
      {record.next_action && (
        <p>
          {copy.nextAction}: {record.next_action}
        </p>
      )}
      {record.needs_mandate_reconciliation && (
        <p role="status">{copy.reconcileNeeded}</p>
      )}
      <Sources sources={record.sources} />
      {record.outcome && (
        <>
          <p className="whitespace-pre-wrap">{record.outcome.statement}</p>
          <Sources sources={record.outcome.sources} />
        </>
      )}
      <p className="text-muted-foreground text-sm">{copy.unchecked}</p>
      {record.review && (
        <p>
          {copy.acceptedBy}: {record.review.actor_id}
        </p>
      )}
      {record.review_state === "reported" && <p>{copy.reportedComplete}</p>}
      {record.blocker && (
        <p className="whitespace-pre-wrap">{record.blocker.question}</p>
      )}
      {(manager || (collaborate && record.status === "blocked")) && (
        <label>
          {copy.note}
          <textarea
            maxLength={4096}
            className="w-full rounded border p-2"
            disabled={disabled}
            value={note}
            onChange={(event) => {
              setNote(event.target.value);
              if (!event.target.value) setNoteBasis(null);
              else if (!noteBasis) setNoteBasis(record);
            }}
          />
        </label>
      )}
      {noteBasis && noteBasis.revision !== record.revision && (
        <Button
          variant="outline"
          disabled={disabled}
          onClick={() => {
            setNote("");
            setNoteBasis(null);
            setAcceptanceBasis(null);
          }}
        >
          {copy.resetResponse}
        </Button>
      )}
      {editing ? (
        <AssignmentForm
          record={editing}
          policy={policy}
          manager={manager}
          disabled={disabled}
          submit={(assignment) => command("edit", assignment, editing)}
          cancel={() => setEditing(null)}
        />
      ) : (
        <div className="flex flex-wrap gap-2">
          {manager && editable && (
            <Button
              variant="outline"
              disabled={disabled}
              onClick={() => setEditing(record)}
            >
              {copy.edit}
            </Button>
          )}
          {manager && editable && record.needs_mandate_reconciliation && (
            <Button
              disabled={disabled || !note.trim()}
              onClick={() => command("reconcile_mandate")}
            >
              {copy.reconcile_mandate}
            </Button>
          )}
          {manager && (editable || record.status === "submitted") && (
            <Button
              variant="outline"
              disabled={disabled}
              onClick={() => command("cancel")}
            >
              {copy.cancel}
            </Button>
          )}
          {manager &&
            (record.status === "completed" ||
              record.status === "cancelled") && (
              <Button
                disabled={disabled || !note.trim()}
                onClick={() => command("reopen")}
              >
                {copy.reopen}
              </Button>
            )}
          {manager && record.status === "submitted" && (
            <Button
              disabled={disabled || !note.trim()}
              onClick={() => command("changes_requested")}
            >
              {copy.changes_requested}
            </Button>
          )}
        </div>
      )}
      {manager &&
        !record.human_input_request_id &&
        record.status === "submitted" &&
        record.outcome && (
          <>
            <label className="flex items-center gap-2">
              <input
                type="checkbox"
                checked={acceptanceBasis?.revision === record.revision}
                disabled={disabled}
                onChange={(event) =>
                  setAcceptanceBasis(event.target.checked ? record : null)
                }
              />
              {copy.acceptUnchecked}
            </label>
            <Button
              disabled={
                disabled || acceptanceBasis?.revision !== record.revision
              }
              onClick={() =>
                command(
                  "accept",
                  {
                    outcome_id: record.outcome!.id,
                    evidence_revision: record.outcome!.evidence_revision,
                    basis: "outcome_statement",
                    acknowledge_unchecked_sources: true,
                  },
                  acceptanceBasis ?? record,
                )
              }
            >
              {copy.accept}
            </Button>
          </>
        )}
      {!record.human_input_request_id &&
        record.status === "blocked" &&
        record.blocker && (
          <>
            <p>{copy.inputNotice}</p>
            {collaborate && (
              <Button
                disabled={disabled || !note.trim()}
                onClick={() =>
                  command("input", {
                    blocker_id: record.blocker!.id,
                    blocker_revision: record.blocker!.revision,
                  })
                }
              >
                {copy.input}
              </Button>
            )}
            {manager && record.blocker.kind === "decision" && (
              <Button
                disabled={disabled || !note.trim()}
                onClick={() =>
                  command("decide", {
                    blocker_id: record.blocker!.id,
                    blocker_revision: record.blocker!.revision,
                  })
                }
              >
                {copy.decide}
              </Button>
            )}
          </>
        )}
      <details onToggle={(event) => setHistoryOpen(event.currentTarget.open)}>
        <summary>{copy.history}</summary>
        {history.isFetching && <p>{t.common.loading}</p>}
        {history.error ? (
          <p role="alert">{history.error.message}</p>
        ) : (
          history.data?.events.map((event) => (
            <div className="my-2 rounded border p-2" key={event.id}>
              <p>
                {copy[event.actor_kind]}: {event.actor_id} ·{" "}
                {new Date(event.created_at).toLocaleString()} ·{" "}
                {copy[event.action as keyof typeof copy] ?? event.action}
              </p>
              <p>
                {event.record.objective} · {copy[event.record.status]}
              </p>
              <p>{event.record.success_criteria}</p>
              {event.note && (
                <p className="whitespace-pre-wrap">{event.note}</p>
              )}
              {event.record.outcome && <p>{event.record.outcome.statement}</p>}
              {event.record.review && (
                <p>
                  {copy.acceptedBy}: {event.record.review.actor_id}
                </p>
              )}
              <Sources sources={event.record.sources} />
            </div>
          ))
        )}
        <Button
          variant="outline"
          disabled={offset === 0}
          onClick={() => setOffset((value) => Math.max(0, value - 50))}
        >
          {copy.previous}
        </Button>
        <Button
          variant="outline"
          disabled={history.data?.events.length !== 50}
          onClick={() => setOffset((value) => value + 50)}
        >
          {copy.next}
        </Button>
      </details>
    </article>
  );
}
