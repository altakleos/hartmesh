"use client";
import { useQueryClient } from "@tanstack/react-query";
import Link from "next/link";
import { useRef, useState } from "react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { usePersonName } from "@/components/workspace/person-name";
import {
  InstanceApiError,
  operationID,
  type WorkRecord,
  type AgentInstance,
} from "@/core/agent-instances/api";
import { createInput, type CreateInput } from "@/core/attention/api";
import { useI18n } from "@/core/i18n/hooks";
import { useSpaceActionSignal } from "@/core/spaces/lifetime";

import { InputRecipientSelect } from "./recipient-select";

export function RequestInput({
  record,
  manager,
  disabled,
  instance,
}: {
  record: WorkRecord;
  manager: boolean;
  disabled: boolean;
  instance: Pick<AgentInstance, "name" | "supervisor">;
}) {
  const { t } = useI18n();
  const c = t.attention;
  const supervisor = usePersonName(instance.supervisor.subject_id) ?? "…";
  const client = useQueryClient();
  const capture = useSpaceActionSignal();
  const [question, setQuestion] = useState(""),
    [reason, setReason] = useState(""),
    [expected, setExpected] = useState(""),
    [recipient, setRecipient] = useState(""),
    [choices, setChoices] = useState("");
  const [purpose, setPurpose] = useState<"information" | "decision">(
    "information",
  );
  const [basis, setBasis] = useState(record.revision);
  const [pending, setPending] = useState<CreateInput | null>(null);
  const [error, setError] = useState("");
  const busy = useRef(false);
  const [working, setWorking] = useState(false);
  const [recipientValid, setRecipientValid] = useState(true);
  const [expanded, setExpanded] = useState(false);
  async function send(body: CreateInput) {
    if (busy.current) return;
    busy.current = true;
    setWorking(true);
    setPending(body);
    setError("");
    const signal = capture();
    try {
      await createInput(record.instance_id, record.id, body, signal);
      if (signal.aborted) return;
      setPending(null);
      setQuestion("");
      setReason("");
      setExpected("");
      await Promise.all([
        client.invalidateQueries({ queryKey: ["agent-work"] }),
        client.invalidateQueries({ queryKey: ["attention"] }),
      ]);
    } catch (e) {
      if (signal.aborted) return;
      const definite =
        e instanceof InstanceApiError && e.status >= 400 && e.status < 500;
      if (definite) setPending(null);
      setError(definite ? t.agentInstances.operationError : c.unconfirmed);
    } finally {
      busy.current = false;
      if (!signal.aborted) setWorking(false);
    }
  }
  return (
    <section className="space-y-3 border-t pt-3">
      <Link
        className="underline"
        href={
          record.human_input_request_id
            ? `/workspace/attention?request=${record.human_input_request_id}`
            : `/workspace/attention?work=${record.id}`
        }
      >
        {c.requestLink}
      </Link>
      {manager &&
        !record.human_input_request_id &&
        ["open", "submitted"].includes(record.status) &&
        record.work_enabled &&
        !record.needs_mandate_reconciliation && (
          <details onToggle={(event) => setExpanded(event.currentTarget.open)}>
            <summary>{c.create}</summary>
            <form
              className="flex flex-col gap-2"
              onSubmit={(e) => {
                e.preventDefault();
                if (
                  !recipientValid ||
                  disabled ||
                  working ||
                  pending ||
                  basis !== record.revision
                )
                  return;
                void send({
                  operation_id: operationID(),
                  expected_work_revision: basis,
                  expected_assignment_revision: record.assignment_revision,
                  purpose: record.status === "submitted" ? "review" : purpose,
                  question,
                  reason,
                  expected_response: expected,
                  choices: choices
                    .split("\n")
                    .map((x) => x.trim())
                    .filter(Boolean),
                  ...(recipient ? { recipient_id: recipient } : {}),
                });
              }}
            >
              <fieldset
                disabled={
                  disabled || working || !!pending || basis !== record.revision
                }
                className="space-y-2"
              >
                <p>{c.shared(instance.name)}</p>
                {record.status === "open" && (
                  <label>
                    {c.purpose}
                    <select
                      value={purpose}
                      onChange={(e) =>
                        setPurpose(e.target.value as typeof purpose)
                      }
                    >
                      <option value="information">{c.information}</option>
                      <option value="decision">{c.decision}</option>
                    </select>
                  </label>
                )}
                <label>
                  {c.question}
                  <Input
                    required
                    maxLength={4096}
                    value={question}
                    onChange={(e) => setQuestion(e.target.value)}
                  />
                </label>
                <label>
                  {c.reason}
                  <Input
                    required
                    maxLength={4096}
                    value={reason}
                    onChange={(e) => setReason(e.target.value)}
                  />
                </label>
                <label>
                  {c.expected}
                  <Input
                    required
                    maxLength={4096}
                    value={expected}
                    onChange={(e) => setExpected(e.target.value)}
                  />
                </label>
                <InputRecipientSelect
                  instance={record.instance_id}
                  purpose={record.status === "submitted" ? "review" : purpose}
                  enabled={manager && expanded}
                  value={recipient}
                  choose={setRecipient}
                  validity={setRecipientValid}
                  defaultLabel={c.recipientDefault(supervisor)}
                />
                <label>
                  {c.choices}
                  <textarea
                    maxLength={4096}
                    value={choices}
                    onChange={(e) => setChoices(e.target.value)}
                  />
                </label>
                <Button type="submit" disabled={!recipientValid}>
                  {c.create}
                </Button>
              </fieldset>
            </form>
            {basis !== record.revision && !pending && (
              <Button
                onClick={() => {
                  setBasis(record.revision);
                  setQuestion("");
                  setReason("");
                  setExpected("");
                  setChoices("");
                }}
              >
                {c.reset}
              </Button>
            )}
          </details>
        )}
      {pending && (
        <Button disabled={working} onClick={() => void send(pending)}>
          {c.retry}
        </Button>
      )}
      {error && <p role="alert">{error}</p>}
    </section>
  );
}
