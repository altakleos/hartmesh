"use client";

import { useQuery } from "@tanstack/react-query";
import { useEffect, useId, useRef, useState } from "react";

import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { prepareBriefDefinition } from "@/core/agent-instances/adopt-brief";
import * as api from "@/core/agent-instances/api";
import { listAgents } from "@/core/agents/api";
import { awaitAbortable } from "@/core/api/abort";
import { useAuth } from "@/core/auth/AuthProvider";
import { useAgentInstances } from "@/core/features";
import { useI18n } from "@/core/i18n/hooks";
import { useSpaceActionSignal } from "@/core/spaces/lifetime";

import { useLifecycleActions } from "./lifecycle-actions";

type Definition = Awaited<ReturnType<typeof api.getInstanceDefinition>>;
export function BriefSettings({
  disabled,
  onBusy,
}: {
  disabled: boolean;
  onBusy: (busy: boolean) => void;
}) {
  const { instance } = useLifecycleActions();
  const { user } = useAuth();
  const { t } = useI18n();
  const definition = useQuery({
    queryKey: [
      "agent-instances",
      user?.id,
      user?.system_role,
      user?.permissions,
      instance.id,
      "definition",
      instance.definition_revision,
      instance.permissions,
    ],
    queryFn: ({ signal }) => api.getInstanceDefinition(instance.id, signal),
    retry: false,
  });
  if (definition.isPending)
    return <p role="status">{t.agentInstances.loading}</p>;
  if (definition.isError)
    return (
      <div role="alert">
        <p>{t.agentInstances.create.briefFailed}</p>
        <Button type="button" onClick={() => void definition.refetch()}>
          {t.agentInstances.refresh}
        </Button>
      </div>
    );
  return (
    <BriefEditor
      key={definition.data.revision}
      definition={definition.data}
      disabled={disabled}
      onBusy={onBusy}
    />
  );
}
function BriefEditor({
  definition,
  disabled,
  onBusy,
}: {
  definition: Definition;
  disabled: boolean;
  onBusy: (busy: boolean) => void;
}) {
  const actions = useLifecycleActions();
  const { instance } = actions;
  const { user } = useAuth();
  const feature = useAgentInstances();
  const { t } = useI18n();
  const copy = t.agentInstances;
  const id = useId();
  const captureSignal = useSpaceActionSignal();
  const [text, setText] = useState(definition.soul);
  const [enabled, setEnabled] = useState(
    definition.config.work_policy?.enabled === true,
  );
  const [custom, setCustom] = useState(false);
  const [source, setSource] = useState("");
  const [busy, setBusy] = useState(false);
  const busyRef = useRef(false);
  useEffect(() => () => onBusy(false), [onBusy]);
  const [error, setError] = useState<string>();
  const definitions = useQuery({
    queryKey: [
      "agents",
      "instance-adoption",
      user?.id,
      user?.system_role,
      user?.permissions,
    ],
    queryFn: ({ signal }) => listAgents(signal),
    enabled: custom && feature.known && feature.enabled,
    retry: false,
  });
  const invalid = !text.trim()
    ? copy.create.briefRequired
    : Array.from(text).length > 16_000
      ? copy.create.briefTooLong
      : undefined;
  const changed = custom
    ? !!source
    : text !== definition.soul ||
      enabled !== (definition.config.work_policy?.enabled === true);
  const frozen = disabled || busy;
  async function apply() {
    if (
      frozen ||
      busyRef.current ||
      !changed ||
      (!custom && invalid) ||
      !actions.canApply("adopt")
    )
      return;
    const owner = captureSignal();
    const deadline = AbortSignal.any([owner, AbortSignal.timeout(20_000)]);
    busyRef.current = true;
    setBusy(true);
    onBusy(true);
    setError(undefined);
    try {
      const name = custom
        ? source
        : await prepareBriefDefinition(
            definition.config,
            text,
            enabled,
            deadline,
          );
      deadline.throwIfAborted();
      await awaitAbortable(
        actions.change({
          action: "adopt",
          definition_name: name,
          only_if_idle: true,
        }),
        owner,
      );
    } catch {
      if (!owner.aborted) setError(copy.create.briefFailed);
    } finally {
      if (!owner.aborted) {
        busyRef.current = false;
        setBusy(false);
        onBusy(false);
      }
    }
  }
  return (
    <section className="space-y-3">
      <h3 className="font-medium">{copy.instructions}</h3>
      {error && <p role="alert">{error}</p>}
      {custom ? (
        <>
          <label htmlFor={`${id}-custom`}>{copy.create.customAgent}</label>
          <select
            id={`${id}-custom`}
            disabled={frozen || definitions.isError}
            value={source}
            onChange={(event) => setSource(event.target.value)}
          >
            <option value="" />
            {!definitions.isError &&
              definitions.data?.map((agent) => (
                <option key={agent.name} value={agent.name}>
                  {agent.name}
                </option>
              ))}
          </select>
          {definitions.isError ? (
            <div role="alert">
              <p>{copy.create.briefFailed}</p>
              <Button
                type="button"
                disabled={frozen}
                onClick={() => void definitions.refetch()}
              >
                {copy.refresh}
              </Button>
            </div>
          ) : (
            definitions.data?.length === 0 && (
              <p>{copy.create.customAgentNone}</p>
            )
          )}
        </>
      ) : (
        <>
          <label htmlFor={`${id}-brief`}>{copy.instructions}</label>
          <Textarea
            id={`${id}-brief`}
            disabled={frozen}
            value={text}
            onChange={(event) => setText(event.target.value)}
          />
          {invalid && <p role="alert">{invalid}</p>}
          <label className="flex items-center gap-2">
            <input
              type="checkbox"
              checked={enabled}
              disabled={frozen}
              onChange={(event) => setEnabled(event.target.checked)}
            />
            {copy.settings.assignments(instance.name)}
          </label>
        </>
      )}
      <p>{copy.settings.briefNote(instance.name)}</p>
      {feature.known && feature.enabled && (
        <Button
          type="button"
          variant="link"
          disabled={frozen}
          onClick={() => setCustom(!custom)}
        >
          {custom ? copy.create.useRole : copy.settings.useCustomAgent}
        </Button>
      )}
      <Button
        type="button"
        disabled={frozen || !changed || (!custom && !!invalid)}
        onClick={() => void apply()}
      >
        {copy.adopt}
      </Button>
      {actions.lastApplied?.body.action === "adopt" && (
        <p role="status">{copy.settings.briefApplied}</p>
      )}
    </section>
  );
}
