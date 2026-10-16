"use client";

import { useQuery } from "@tanstack/react-query";
import { useId, useState } from "react";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import {
  listInstanceRoles,
  type Custody,
  type InstanceRole,
} from "@/core/agent-instances/api";
import { listAgents } from "@/core/agents/api";
import { useAuth } from "@/core/auth/AuthProvider";
import { useAgentInstances } from "@/core/features";
import { useI18n } from "@/core/i18n/hooks";

export interface BriefDraft {
  name: string;
  brief: string;
  role: InstanceRole | null;
  useCustom: boolean;
  customDefinition: string | null;
}

export function BriefStep({
  kind,
  value,
  onChange,
  disabled = false,
}: {
  kind: Custody;
  value: BriefDraft;
  onChange: (value: BriefDraft) => void;
  disabled?: boolean;
}) {
  const { user } = useAuth();
  const feature = useAgentInstances();
  const { t } = useI18n();
  const copy = t.agentInstances.create;
  const id = useId();
  const enabled = feature.known && feature.enabled && !!user;
  const scope = [user?.id, user?.system_role, user?.permissions];
  const roles = useQuery({
    queryKey: ["agent-instance-roles", ...scope],
    queryFn: ({ signal }) => listInstanceRoles(signal),
    enabled,
    retry: false,
  });
  const definitions = useQuery({
    queryKey: ["agents", "creation", ...scope],
    queryFn: ({ signal }) => listAgents(signal),
    enabled: enabled && value.useCustom,
    retry: false,
  });
  const [edited, setEdited] = useState(value.brief.length > 0);
  const [replacement, setReplacement] = useState<{
    role: InstanceRole | null;
  }>();
  const name = value.name.trim();
  const nameInvalid =
    Array.from(name).length < 1 || Array.from(name).length > 128;
  const briefInvalid = !value.brief.trim()
    ? copy.briefRequired
    : Array.from(value.brief).length > 16_000
      ? copy.briefTooLong
      : undefined;

  function apply(role: InstanceRole | null, keep: boolean) {
    if (disabled) return;
    onChange({
      ...value,
      role,
      name: name ? value.name : (role?.name ?? ""),
      brief: keep ? value.brief : (role?.soul ?? ""),
    });
    if (!keep) setEdited(false);
    setReplacement(undefined);
  }
  function select(role: InstanceRole | null) {
    if (edited && value.brief.trim() && value.brief !== (role?.soul ?? ""))
      setReplacement({ role });
    else apply(role, false);
  }

  return (
    <div className="space-y-5">
      <div className="space-y-2">
        <label htmlFor={`${id}-name`}>{t.agentInstances.name}</label>
        <Input
          id={`${id}-name`}
          value={value.name}
          disabled={disabled}
          placeholder={copy.namePlaceholder}
          aria-describedby={`${id}-name-help${nameInvalid ? ` ${id}-name-error` : ""}`}
          aria-invalid={nameInvalid}
          onChange={(event) => onChange({ ...value, name: event.target.value })}
        />
        <p id={`${id}-name-help`} className="text-muted-foreground text-sm">
          {copy.nameHelp(kind)}
        </p>
        {nameInvalid && (
          <p id={`${id}-name-error`} role="alert">
            {copy.nameInvalid}
          </p>
        )}
      </div>
      {value.useCustom ? (
        <div className="space-y-2">
          <label htmlFor={`${id}-definition`}>{copy.customAgent}</label>
          {definitions.isPending && <p role="status">{t.common.loading}</p>}
          {definitions.isError && (
            <div role="alert">
              <p>{copy.briefFailed}</p>
              <Button
                type="button"
                variant="outline"
                disabled={disabled || definitions.isFetching}
                onClick={() => void definitions.refetch()}
              >
                {t.agentInstances.refresh}
              </Button>
            </div>
          )}
          {definitions.isSuccess && !definitions.data.length && (
            <p>{copy.customAgentNone}</p>
          )}
          <select
            id={`${id}-definition`}
            className="border-input bg-background w-full rounded-md border p-2"
            value={value.customDefinition ?? ""}
            disabled={disabled || !enabled || !definitions.data?.length}
            onChange={(event) =>
              onChange({
                ...value,
                customDefinition: event.target.value || null,
              })
            }
          >
            <option value="" />
            {(definitions.data ?? []).map((definition) => (
              <option key={definition.name} value={definition.name}>
                {definition.name}
              </option>
            ))}
          </select>
        </div>
      ) : (
        <>
          <div className="space-y-2">
            <h3 className="font-medium">
              {name ? copy.roleHeading(name) : copy.roleHeadingUnnamed(kind)}
            </h3>
            {roles.isFetching && <p role="status">{t.common.loading}</p>}
            {roles.isError && (
              <div role="alert">
                <p>{copy.rolesFailed}</p>
                <Button
                  type="button"
                  variant="outline"
                  disabled={disabled || roles.isFetching}
                  onClick={() => void roles.refetch()}
                >
                  {t.agentInstances.retry}
                </Button>
              </div>
            )}
            {roles.isSuccess && !roles.data.roles.length && (
              <p>{copy.rolesNone}</p>
            )}
            <div className="grid gap-2 sm:grid-cols-2">
              {(roles.isError ? [] : (roles.data?.roles ?? [])).map((role) => (
                <button
                  type="button"
                  className="hover:bg-muted aria-pressed:bg-muted rounded-md border p-3 text-left disabled:opacity-50"
                  key={role.id}
                  aria-pressed={value.role?.id === role.id}
                  disabled={disabled}
                  onClick={() => select(role)}
                >
                  <span className="block font-medium">{role.name}</span>
                  <span className="text-muted-foreground text-sm">
                    {role.description}
                  </span>
                </button>
              ))}
              <button
                type="button"
                className="hover:bg-muted aria-pressed:bg-muted rounded-md border p-3 text-left disabled:opacity-50"
                aria-pressed={value.role === null}
                disabled={disabled}
                onClick={() => select(null)}
              >
                <span className="block font-medium">{copy.roleBlank}</span>
                <span className="text-muted-foreground text-sm">
                  {copy.roleBlankDescription}
                </span>
              </button>
            </div>
          </div>
          <div className="space-y-2">
            <label htmlFor={`${id}-brief`}>
              {name ? copy.brief(name) : copy.briefUnnamed(kind)}
            </label>
            <Textarea
              id={`${id}-brief`}
              className="min-h-36"
              value={value.brief}
              disabled={disabled}
              placeholder={copy.briefPlaceholder}
              aria-describedby={`${id}-brief-help${briefInvalid ? ` ${id}-brief-error` : ""}`}
              aria-invalid={!!briefInvalid}
              onChange={(event) => {
                setEdited(true);
                onChange({ ...value, brief: event.target.value });
              }}
            />
            <p
              id={`${id}-brief-help`}
              className="text-muted-foreground text-sm"
            >
              {name ? copy.briefHelp(name) : copy.briefHelpUnnamed(kind)}
            </p>
            {briefInvalid && (
              <p id={`${id}-brief-error`} role="alert">
                {briefInvalid}
              </p>
            )}
          </div>
        </>
      )}
      {enabled && (
        <Button
          type="button"
          variant="link"
          className="px-0"
          disabled={disabled}
          onClick={() => onChange({ ...value, useCustom: !value.useCustom })}
        >
          {value.useCustom ? copy.useRole : copy.useCustomAgent}
        </Button>
      )}
      <Dialog
        open={!!replacement && !disabled}
        onOpenChange={(open) => {
          if (!open) setReplacement(undefined);
        }}
      >
        <DialogContent aria-describedby={undefined}>
          <DialogTitle>{copy.replaceBrief}</DialogTitle>
          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              onClick={() => replacement && apply(replacement.role, true)}
            >
              {copy.keepBrief}
            </Button>
            <Button
              type="button"
              onClick={() => replacement && apply(replacement.role, false)}
            >
              {copy.useRoleBrief}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
