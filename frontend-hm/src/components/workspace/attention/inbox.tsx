"use client";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import Link from "next/link";
import { useRef, useState } from "react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { ActorName } from "@/components/workspace/actor-name";
import { AIBadge } from "@/components/workspace/instances/ai-badge";
import * as workAPI from "@/core/agent-instances/api";
import * as api from "@/core/attention/api";
import { useAttention } from "@/core/attention/hooks";
import { useAuth } from "@/core/auth/AuthProvider";
import { useI18n } from "@/core/i18n/hooks";
import { listSpaces } from "@/core/spaces/api";
import { useSpaceActionSignal } from "@/core/spaces/lifetime";

import { InputRecipientSelect, NO_RECIPIENT } from "./recipient-select";

export function AttentionInbox({
  work,
  initialRequest,
  executionUnavailable = false,
}: {
  work?: string;
  initialRequest?: string;
  executionUnavailable?: boolean;
}) {
  const { t } = useI18n();
  const c = t.attention;
  const [view, setView] = useState<api.InboxView>(work ? "all" : "pending"),
    [offset, setOffset] = useState(0),
    [selected, setSelected] = useState(initialRequest);
  const query = useAttention(view, offset, work);
  return (
    <div className="space-y-4">
      {!selected && (
        <>
          <p>{c.notice()}</p>
          {executionUnavailable && <p>{c.unavailableStill()}</p>}
        </>
      )}
      <nav className="flex flex-wrap gap-2" aria-label={c.title}>
        {(["pending", "routing", "answered", "all"] as const).map((v) => (
          <Button
            key={v}
            aria-pressed={view === v}
            variant={view === v ? "default" : "outline"}
            onClick={() => {
              setView(v);
              setOffset(0);
              setSelected(undefined);
            }}
          >
            {c[v]}
            {v !== "all" && !query.isError && query.data?.counts
              ? ` (${query.data.counts[v]})`
              : ""}
          </Button>
        ))}
      </nav>
      <Button variant="outline" onClick={() => void query.refetch()}>
        {c.refresh}
      </Button>
      {query.isError ? (
        <p role="alert">{c.loadError}</p>
      ) : query.isPending ? (
        <p>{t.common.loading}</p>
      ) : (
        <>
          <ul className="space-y-2">
            {query.data.requests.map((r) => (
              <li key={r.id}>
                <Button
                  variant="outline"
                  className="h-auto w-full justify-start text-left whitespace-normal"
                  onClick={() => setSelected(r.id)}
                >
                  <AIBadge /> {r.instance_name} · {r.work_objective} —{" "}
                  {r.question}
                </Button>
              </li>
            ))}
          </ul>
          {!query.data.requests.length && (
            <p>{view === "pending" ? c.emptyPending : c.empty}</p>
          )}
          <div className="flex gap-2">
            <Button
              disabled={!offset}
              onClick={() => setOffset((o) => Math.max(0, o - 50))}
            >
              {c.back}
            </Button>
            <Button
              disabled={!query.data.has_more}
              onClick={() => setOffset((o) => o + 50)}
            >
              {c.next}
            </Button>
          </div>
        </>
      )}
      {selected && (
        <RequestDetail
          key={selected}
          id={selected}
          executionUnavailable={executionUnavailable}
        />
      )}
    </div>
  );
}

type Intent =
  | { kind: "respond"; body: api.Reply }
  | { kind: "command"; body: api.RequestCommand }
  | { kind: "work"; body: workAPI.WorkCommand };
export function RequestDetail({
  id,
  executionUnavailable = false,
}: {
  id: string;
  executionUnavailable?: boolean;
}) {
  const { user } = useAuth();
  const { t } = useI18n();
  const c = t.attention;
  const client = useQueryClient();
  const capture = useSpaceActionSignal();
  const key = [
    "attention-detail",
    user?.id,
    user?.system_role,
    user?.permissions,
    id,
  ];
  const query = useQuery({
    queryKey: key,
    queryFn: async ({ signal }) => {
      const request = await api.getInput(id, signal);
      const [first, second, work] = await Promise.all([
        api.getResponses(id, 0, signal),
        api.getResponses(id, 100, signal),
        workAPI.getWork(request.instance_id, request.work_id, signal),
      ]);
      return {
        request,
        responses: [...first.responses, ...second.responses],
        work,
      };
    },
    retry: false,
    gcTime: 0,
    staleTime: 0,
    refetchInterval: 30000,
    refetchIntervalInBackground: false,
  });
  const [text, setText] = useState(""),
    [choice, setChoice] = useState(""),
    [disposition, setDisposition] =
      useState<api.Reply["disposition"]>("supplied");
  const [recipient, setRecipient] = useState(""),
    [note, setNote] = useState(""),
    [space, setSpace] = useState(""),
    [path, setPath] = useState("");
  const [recipientValid, setRecipientValid] = useState(false);
  const [draftBasis, setDraftBasis] = useState<{
    request: number;
    assignment: number;
  } | null>(null);
  const [ack, setAck] = useState<number | null>(null),
    [decisionBasis, setDecisionBasis] = useState<number | null>(null);
  const [routingBasis, setRoutingBasis] = useState<{
    revision: number;
    request_revision: number;
  } | null>(null);
  const [pending, setPending] = useState<Intent | null>(null),
    [busy, setBusy] = useState(false),
    [error, setError] = useState(""),
    [receipt, setReceipt] = useState("");
  const busyRef = useRef(false);
  const [historyOffset, setHistoryOffset] = useState<number | null>(null);
  const history = useQuery({
    queryKey: [...key, "history", historyOffset],
    queryFn: ({ signal }) => api.getHistory(id, historyOffset ?? 0, signal),
    enabled: historyOffset !== null && !query.isError,
    retry: false,
    gcTime: 0,
  });
  const spaces = useQuery({
    queryKey: [...key, "spaces"],
    queryFn: ({ signal }) => listSpaces(signal),
    enabled: !!query.data?.request.can_respond && !query.isError,
    retry: false,
    gcTime: 0,
  });
  const current = query.data;
  function captureDraft() {
    if (current && !draftBasis)
      setDraftBasis({
        request: current.request.request_revision,
        assignment: current.request.assignment_revision,
      });
  }
  async function execute(intent: Intent) {
    if (busyRef.current || !current) return;
    busyRef.current = true;
    setBusy(true);
    setPending(intent);
    setError("");
    setReceipt("");
    const signal = capture();
    try {
      if (intent.kind === "respond") await api.respond(id, intent.body, signal);
      else if (intent.kind === "command")
        await api.command(id, intent.body, signal);
      else
        await workAPI.commandWork(
          current.request.instance_id,
          current.request.work_id,
          intent.body,
          signal,
        );
      if (signal.aborted) return;
      setPending(null);
      setRecipient("");
      setRoutingBasis(null);
      setText("");
      setChoice("");
      setNote("");
      setPath("");
      setSpace("");
      setDraftBasis(null);
      setDecisionBasis(null);
      setAck(null);
      setReceipt(c.savedAt(new Date().toLocaleTimeString()));
      await Promise.all([
        client.invalidateQueries({ queryKey: ["attention"] }),
        client.invalidateQueries({ queryKey: ["attention-detail"] }),
        client.invalidateQueries({ queryKey: ["agent-work"] }),
      ]);
    } catch (e) {
      if (signal.aborted) return;
      const definite =
        e instanceof workAPI.InstanceApiError &&
        e.status >= 400 &&
        e.status < 500;
      if (definite) setPending(null);
      setError(definite ? t.agentInstances.operationError : c.unconfirmed);
      if (definite) await query.refetch();
    } finally {
      busyRef.current = false;
      if (!signal.aborted) setBusy(false);
    }
  }
  if (query.isError)
    return (
      <section>
        <p role="alert">{c.loadError}</p>
        <Button onClick={() => void query.refetch()}>{c.refresh}</Button>
      </section>
    );
  if (!current) return <p>{t.common.loading}</p>;
  const { request: r, responses, work } = current;
  const stale =
    !!draftBasis &&
    (draftBasis.request !== r.request_revision ||
      draftBasis.assignment !== r.assignment_revision);
  const disabled = busy || !!pending;
  const commandBase = {
    operation_id: workAPI.operationID(),
    expected_revision: r.revision,
    expected_request_revision: r.request_revision,
  };
  const requestBasis = {
    id: r.id,
    revision: r.revision,
    request_revision: r.request_revision,
    response_ids: responses.map((x) => x.id),
  };
  const canonical = (action: "decide" | "accept"): workAPI.WorkCommand => ({
    operation_id: workAPI.operationID(),
    expected_revision: work.revision,
    expected_assignment_revision: work.assignment_revision,
    action,
    request_basis: requestBasis,
    ...(note ? { note } : {}),
    ...(action === "decide"
      ? { blocker_id: r.basis_id, blocker_revision: r.basis_revision }
      : {
          outcome_id: r.basis_id,
          evidence_revision: r.basis_revision,
          basis: "outcome_statement",
          acknowledge_unchecked_sources: true,
        }),
  });
  return (
    <section aria-label={r.question} className="space-y-3 rounded border p-4">
      <h2 className="text-xl font-semibold">{r.question}</h2>
      <p>{c.notice(r.instance_name)}</p>
      {executionUnavailable && <p>{c.unavailableStill(r.instance_name)}</p>}
      <p>
        <AIBadge /> {r.instance_name} · {r.work_objective}
      </p>
      <p>
        {c.reason}: {r.reason}
      </p>
      <p>
        {c.expected}: {r.expected_response}
      </p>
      <p>
        {c.state}: {c[r.state === "pending" ? "pending" : r.state]}
        {r.closed_reason && Object.hasOwn(c.closedReason, r.closed_reason)
          ? ` (${c.closedReason[r.closed_reason as keyof typeof c.closedReason]})`
          : ""}
      </p>
      <p>
        {c.activity}: {new Date(r.updated_at).toLocaleString()}
      </p>
      <p>
        {c.recipient}:{" "}
        {r.needs_routing || !r.recipient_id ? (
          c.recipientMissing
        ) : (
          <ActorName id={r.recipient_id} />
        )}
      </p>
      {r.read_revision > 0 && (
        <p>{r.read_revision === r.revision ? c.readCurrent : c.readStale}</p>
      )}
      <Link
        className="underline"
        href={`/workspace/instances?instance=${r.instance_id}&tab=work&work=${r.work_id}`}
      >
        {c.openWork}
      </Link>
      <Button
        disabled={busy}
        variant="outline"
        onClick={() => {
          const signal = capture();
          void api
            .markRead(id, r.revision, signal)
            .then(() => {
              if (!signal.aborted) void query.refetch();
            })
            .catch(() => {
              if (!signal.aborted) setError(c.unconfirmed);
            });
        }}
      >
        {c.read}
      </Button>
      <p>{c.readNotice}</p>
      {r.state === "answered" && <p>{c.noAssessment(r.instance_name)}</p>}
      <SourceList sources={r.sources} />
      <h3>{c.responses}</h3>
      <ul className="space-y-2">
        {responses.map((response) => (
          <li key={response.id} className="border-l pl-3">
            <p>
              <ActorName id={response.actor_id} kind={response.actor_kind} /> ·{" "}
              {new Date(response.created_at).toLocaleString()} ·{" "}
              {c[response.disposition]}
            </p>
            <p className="whitespace-pre-wrap">{response.text}</p>
            <p>{response.choice}</p>
            <SourceList sources={response.sources} />
          </li>
        ))}
      </ul>
      {r.can_respond && (
        <form
          className="space-y-3"
          onSubmit={(e) => {
            e.preventDefault();
            void execute({
              kind: "respond",
              body: {
                operation_id: workAPI.operationID(),
                expected_request_revision:
                  draftBasis?.request ?? r.request_revision,
                expected_assignment_revision:
                  draftBasis?.assignment ?? r.assignment_revision,
                disposition,
                ...(text.trim() ? { text } : {}),
                ...(choice ? { choice } : {}),
                ...(space && path
                  ? { sources: [{ kind: "space_file", space_id: space, path }] }
                  : {}),
              },
            });
          }}
        >
          <fieldset
            disabled={disabled || stale}
            className="flex flex-col gap-3"
          >
            <p>{c.shared(r.instance_name)}</p>
            <label>
              {c.text}
              <textarea
                className="w-full rounded border p-2"
                maxLength={4096}
                value={text}
                onChange={(e) => {
                  captureDraft();
                  setText(e.target.value);
                }}
              />
            </label>
            {r.choices.length > 0 && (
              <label>
                {c.choice}
                <select
                  value={choice}
                  onChange={(e) => {
                    captureDraft();
                    setChoice(e.target.value);
                  }}
                >
                  <option value="">—</option>
                  {r.choices.map((v) => (
                    <option key={v}>{v}</option>
                  ))}
                </select>
              </label>
            )}
            <label>
              {c.disposition}
              <select
                value={disposition}
                onChange={(e) => {
                  captureDraft();
                  setDisposition(e.target.value as typeof disposition);
                }}
              >
                {(
                  ["supplied", "cannot_provide", "wrong_recipient"] as const
                ).map((v) => (
                  <option key={v} value={v}>
                    {c[v]}
                  </option>
                ))}
              </select>
            </label>
            <details>
              <summary>{c.file}</summary>
              <p>{c.fileNotice(r.instance_name)}</p>
              <label>
                {c.space}
                <select
                  value={space}
                  onChange={(e) => {
                    captureDraft();
                    setSpace(e.target.value);
                  }}
                >
                  <option value="">—</option>
                  {!spaces.isError &&
                    spaces.data?.spaces
                      .filter((s) => (s.permissions & 1) !== 0)
                      .map((s) => (
                        <option key={s.id} value={s.id}>
                          {s.name}
                        </option>
                      ))}
                </select>
              </label>
              <label>
                {c.path}
                <Input
                  maxLength={1024}
                  value={path}
                  onChange={(e) => {
                    captureDraft();
                    setPath(e.target.value);
                  }}
                />
              </label>
              <Link href="/workspace/spaces">{t.storageSpaces.title}</Link>
            </details>
            <Button
              type="submit"
              disabled={!text.trim() && !choice && !(space && path)}
            >
              {c.send}
            </Button>
          </fieldset>
        </form>
      )}
      {stale && <p role="alert">{c.stale}</p>}
      {(stale || decisionBasis !== null || routingBasis !== null) &&
        !pending && (
          <Button
            onClick={() => {
              setText("");
              setChoice("");
              setSpace("");
              setPath("");
              setNote("");
              setDraftBasis(null);
              setRecipient("");
              setRoutingBasis(null);
              setDecisionBasis(null);
              setAck(null);
              void query.refetch();
            }}
          >
            {c.reset}
          </Button>
        )}
      {r.can_manage && (
        <fieldset disabled={disabled} className="space-y-3 border-t pt-3">
          <InputRecipientSelect
            instance={r.instance_id}
            purpose={r.purpose}
            enabled={r.can_manage}
            value={recipient}
            validity={setRecipientValid}
            choose={(value) => {
              setRoutingBasis(
                (b) =>
                  b ?? {
                    revision: r.revision,
                    request_revision: r.request_revision,
                  },
              );
              setRecipient(value);
            }}
          />
          <Button
            disabled={
              !recipientValid ||
              routingBasis?.revision !== r.revision ||
              routingBasis?.request_revision !== r.request_revision
            }
            onClick={() =>
              void execute({
                kind: "command",
                body: {
                  ...commandBase,
                  action: "route",
                  recipient_id: recipient === NO_RECIPIENT ? null : recipient,
                },
              })
            }
          >
            {c.route}
          </Button>
          <label>
            {c.note}
            <textarea
              maxLength={4096}
              className="w-full rounded border p-2"
              value={note}
              onChange={(e) => {
                setDecisionBasis((b) => b ?? r.revision);
                setNote(e.target.value);
              }}
            />
          </label>
          {r.purpose === "decision" && (
            <Button
              disabled={!note.trim() || decisionBasis !== r.revision}
              onClick={() =>
                void execute({ kind: "work", body: canonical("decide") })
              }
            >
              {c.decide}
            </Button>
          )}
          {r.purpose === "review" && (
            <>
              <h3>{c.outcome}</h3>
              <p>{work.outcome?.statement}</p>
              <SourceList sources={work.outcome?.sources ?? []} />
              <label>
                <input
                  type="checkbox"
                  checked={ack === r.revision}
                  onChange={(e) => setAck(e.target.checked ? r.revision : null)}
                />
                {c.ack}
              </label>
              <Button
                disabled={ack !== r.revision}
                onClick={() =>
                  void execute({ kind: "work", body: canonical("accept") })
                }
              >
                {c.accept}
              </Button>
            </>
          )}
          {r.purpose !== "review" && (
            <Button
              variant="outline"
              disabled={!note.trim() || decisionBasis !== r.revision}
              onClick={() =>
                void execute({
                  kind: "command",
                  body: {
                    ...commandBase,
                    action: "withdraw",
                    note,
                    response_ids: responses.map((x) => x.id),
                  },
                })
              }
            >
              {c.withdraw}
            </Button>
          )}
        </fieldset>
      )}
      {pending &&
        (pending.kind === "respond"
          ? r.can_recover_response
          : r.can_recover_management) && (
          <Button disabled={busy} onClick={() => void execute(pending)}>
            {c.retry}
          </Button>
        )}
      {error && <p role="alert">{error}</p>}
      {receipt && <p role="status">{receipt}</p>}
      <Button variant="outline" onClick={() => setHistoryOffset(0)}>
        {c.history}
      </Button>
      {historyOffset !== null &&
        (history.isError ? (
          <p role="alert">{c.loadError}</p>
        ) : (
          <>
            <ul>
              {history.data?.events.map((e) => (
                <li key={e.id}>
                  <ActorName id={e.actor_id} kind={e.actor_kind} /> ·{" "}
                  {Object.hasOwn(c.event, e.action)
                    ? c.event[e.action as keyof typeof c.event]
                    : c.event.other}{" "}
                  · {new Date(e.created_at).toLocaleString()}
                </li>
              ))}
            </ul>
            <Button
              disabled={historyOffset === 0}
              onClick={() =>
                setHistoryOffset((o) => Math.max(0, (o ?? 0) - 50))
              }
            >
              {c.back}
            </Button>
            <Button
              disabled={!history.data?.has_more}
              onClick={() => setHistoryOffset((o) => (o ?? 0) + 50)}
            >
              {c.next}
            </Button>
          </>
        ))}
    </section>
  );
}
function SourceList({ sources }: { sources: api.InputRequest["sources"] }) {
  const { t } = useI18n();
  return (
    <ul>
      {sources.map((s, i) => (
        <li key={i}>
          {s.kind === "space_file" ? (
            <Link
              className="underline"
              href={`/workspace/spaces?space=${s.space_id}`}
            >
              {s.path} · {t.attention.unchecked}
            </Link>
          ) : (
            t.attention.sourceUnavailable
          )}
        </li>
      ))}
    </ul>
  );
}
