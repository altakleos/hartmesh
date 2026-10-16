"use client";

import { useQuery } from "@tanstack/react-query";
import { useId, useState } from "react";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogTitle,
} from "@/components/ui/dialog";
import { PeoplePicker } from "@/components/workspace/people-picker";
import { usePersonNames } from "@/components/workspace/person-name";
import * as api from "@/core/agent-instances/api";
import {
  agentBlock,
  USE,
  INSPECT,
  MANAGE,
} from "@/core/agent-instances/presentation";
import { useAuth } from "@/core/auth/AuthProvider";
import { useAgentInstances } from "@/core/features";
import { useI18n } from "@/core/i18n/hooks";
import type { Person } from "@/core/people/api";
import { usePeople } from "@/core/people/hooks";

import { AgentAvailabilityNotice } from "./agent-header";
import {
  LifecycleFailure,
  useLifecycleActions,
  useLifecycleModal,
} from "./lifecycle-actions";

type Grant = { user_id: string; permissions: number };
export function AccessTab() {
  const actions = useLifecycleActions();
  const { instance } = actions;
  const { user } = useAuth();
  const { t } = useI18n();
  return (instance.permissions & INSPECT) === 0 ? (
    <p>{t.agentInstances.access.manageNeedsInspect(instance.name)}</p>
  ) : (
    <AccessControls
      key={`${user?.id}:${user?.system_role}:${user?.permissions?.join(",")}:${instance.id}:${instance.permissions}`}
    />
  );
}
function AccessControls() {
  const actions = useLifecycleActions();
  const { instance } = actions;
  const { user } = useAuth();
  const feature = useAgentInstances();
  const { t } = useI18n();
  const copy = t.agentInstances.access;
  const [adding, setAdding] = useState(false);
  const [chosen, setChosen] = useState<Person | null>(null);
  const [supervising, setSupervising] = useState(false);
  useLifecycleModal(supervising);
  const [supervisor, setSupervisor] = useState(instance.supervisor.subject_id);
  const id = useId();
  const grants = useQuery({
    queryKey: [
      "agent-instances",
      user?.id,
      user?.system_role,
      user?.permissions,
      instance.id,
      "grants",
      instance.generation,
      instance.permissions,
    ],
    queryFn: ({ signal }) => api.getInstanceGrants(instance.id, signal),
    retry: false,
  });
  const entries = !grants.isError ? grants.data?.grants : undefined;
  const people = usePeople((entries ?? []).map((entry) => entry.user_id));
  const names = usePersonNames([
    instance.supervisor.subject_id,
    ...(entries ?? []).map((entry) => entry.user_id),
  ]);
  const managers =
    entries?.filter((grant) => (grant.permissions & MANAGE) !== 0) ?? [];
  const block = agentBlock(instance, feature);
  const disabled =
    !actions.canManage ||
    actions.pending ||
    actions.busy ||
    !!block ||
    !entries;
  const latest = actions.lastApplied;
  function personName(person: string) {
    return names.get(person) ?? "…";
  }
  return (
    <section className="space-y-5">
      <h2 className="text-lg font-medium">{copy.heading(instance.name)}</h2>
      {block && <AgentAvailabilityNotice instance={instance} block={block} />}
      <div className="space-y-2">
        <h3>
          {t.agentInstances.supervisor}:{" "}
          {personName(instance.supervisor.subject_id)}
        </h3>
        <p>{copy.supervisorNote(instance.name)}</p>
        <Button
          type="button"
          variant="outline"
          disabled={disabled}
          onClick={() => {
            setSupervisor(instance.supervisor.subject_id);
            setSupervising(true);
          }}
        >
          {t.agentInstances.supervise}
        </Button>
        {latest?.body.action === "supervise" && (
          <p role="status">{copy.saved}</p>
        )}
      </div>
      <div className="text-muted-foreground space-y-1 text-sm">
        <p>{copy.useHelp(instance.name)}</p>
        <p>{copy.inspectHelp(instance.name)}</p>
        <p>{copy.manageHelp(instance.name)}</p>
      </div>
      {grants.isPending ? (
        <p role="status">{t.agentInstances.loading}</p>
      ) : grants.isError ? (
        <div role="alert">
          <p>{t.agentInstances.operationError}</p>
          <Button type="button" onClick={() => void grants.refetch()}>
            {t.agentInstances.refresh}
          </Button>
        </div>
      ) : (
        entries?.map((grant) => (
          <GrantRow
            key={`${grant.user_id}:${grant.permissions}:${instance.generation}`}
            grant={grant}
            name={personName(grant.user_id)}
            email={people.get(grant.user_id)?.person?.email}
            disabled={disabled}
            own={grant.user_id === user?.id}
            lastManager={
              !!(grant.permissions & MANAGE) && managers.length === 1
            }
          />
        ))
      )}
      {entries?.length === 1 && entries[0]?.user_id === user?.id && (
        <p>{copy.onlyYou}</p>
      )}
      <p>{copy.filesFollow(instance.name)}</p>
      <Button
        type="button"
        variant="outline"
        disabled={disabled}
        onClick={() => setAdding(!adding)}
      >
        {copy.add}
      </Button>
      {adding && (
        <PeoplePicker
          disabled={disabled}
          existingIds={(entries ?? []).map((grant) => grant.user_id)}
          value={chosen}
          onChange={setChosen}
        />
      )}
      {chosen && !entries?.some((grant) => grant.user_id === chosen.id) && (
        <GrantRow
          key={chosen.id}
          grant={{ user_id: chosen.id, permissions: 0 }}
          initialPermissions={USE | INSPECT}
          name={chosen.name?.trim() ? chosen.name.trim() : chosen.email}
          email={chosen.email}
          disabled={disabled}
          own={false}
          lastManager={false}
        />
      )}
      <Dialog
        open={supervising}
        onOpenChange={(open) => {
          if (!actions.busy) setSupervising(open);
        }}
      >
        <DialogContent>
          <DialogTitle>{t.agentInstances.supervise}</DialogTitle>
          {!!actions.failure && <LifecycleFailure error={actions.failure} />}
          <DialogDescription>
            {copy.supervisorHint(instance.name)}
          </DialogDescription>
          <label htmlFor={id}>{t.agentInstances.supervisor}</label>
          <select
            id={id}
            value={supervisor}
            disabled={disabled}
            onChange={(event) => setSupervisor(event.target.value)}
          >
            {managers.map((grant) => (
              <option key={grant.user_id} value={grant.user_id}>
                {personName(grant.user_id)}
              </option>
            ))}
          </select>
          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              disabled={actions.busy}
              onClick={() => setSupervising(false)}
            >
              {t.common.cancel}
            </Button>
            <Button
              type="button"
              disabled={
                disabled ||
                !managers.some((grant) => grant.user_id === supervisor) ||
                supervisor === instance.supervisor.subject_id
              }
              onClick={() => {
                void actions
                  .change({
                    action: "supervise",
                    supervisor_id: supervisor,
                    only_if_idle: true,
                  })
                  .then((outcome) => {
                    if (outcome !== "refused") setSupervising(false);
                  });
              }}
            >
              {t.common.save}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </section>
  );
}
function GrantRow({
  grant,
  name,
  email,
  disabled,
  own,
  lastManager,
  initialPermissions,
}: {
  grant: Grant;
  name: string;
  email?: string;
  disabled: boolean;
  own: boolean;
  lastManager: boolean;
  initialPermissions?: number;
}) {
  const actions = useLifecycleActions();
  const { t } = useI18n();
  const copy = t.agentInstances.access;
  const [permissions, setPermissions] = useState(
    initialPermissions ?? grant.permissions,
  );
  const [removing, setRemoving] = useState(false);
  useLifecycleModal(removing);
  const manageLocked = (own || lastManager) && !!(grant.permissions & MANAGE);
  const manager = !!(permissions & MANAGE);
  const latest = actions.lastApplied;
  const saved =
    latest?.body.action === "grant" && latest.body.member_id === grant.user_id;
  return (
    <fieldset aria-label={name} className="space-y-2 rounded border p-3">
      <legend>{name}</legend>
      {email && <p className="text-muted-foreground text-sm">{email}</p>}
      <div className="flex flex-wrap gap-4">
        {[USE, INSPECT, MANAGE].map((bit) => (
          <label key={bit} className="flex items-center gap-2">
            <input
              type="checkbox"
              checked={!!(permissions & bit)}
              disabled={
                disabled ||
                (bit === MANAGE && manageLocked) ||
                (bit === INSPECT && manager)
              }
              onChange={(event) =>
                setPermissions((current) =>
                  event.target.checked
                    ? current | bit | (bit === MANAGE ? INSPECT : 0)
                    : current & ~bit,
                )
              }
            />
            {bit === USE
              ? copy.use
              : bit === INSPECT
                ? copy.inspect
                : copy.manage}
          </label>
        ))}
      </div>
      {manager && (
        <p className="text-muted-foreground text-sm">{copy.manageIncludes}</p>
      )}
      {manageLocked && (
        <p>{own ? copy.ownManage : copy.lastManager(actions.instance.name)}</p>
      )}
      {!!(permissions & USE) && !(permissions & INSPECT) && (
        <p>{copy.chatOnlyNote(name, actions.instance.name)}</p>
      )}
      <div className="flex gap-2">
        {permissions !== grant.permissions && (
          <Button
            type="button"
            disabled={disabled || permissions === 0}
            onClick={() =>
              void actions.change({
                action: "grant",
                member_id: grant.user_id,
                permissions,
                only_if_idle: true,
              })
            }
          >
            {t.common.save}
          </Button>
        )}
        {grant.permissions !== 0 && (
          <Button
            type="button"
            variant="outline"
            disabled={disabled || manageLocked}
            onClick={() => setRemoving(true)}
          >
            {copy.remove}
          </Button>
        )}
      </div>
      {saved && <p role="status">{copy.saved}</p>}
      <Dialog
        open={removing}
        onOpenChange={(open) => {
          if (!actions.busy) setRemoving(open);
        }}
      >
        <DialogContent>
          <DialogTitle>{copy.remove}</DialogTitle>
          {!!actions.failure && <LifecycleFailure error={actions.failure} />}
          <DialogDescription>
            {copy.removeConfirm(name, actions.instance.name)}
          </DialogDescription>
          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              disabled={actions.busy}
              onClick={() => setRemoving(false)}
            >
              {t.common.cancel}
            </Button>
            <Button
              type="button"
              disabled={disabled || manageLocked}
              onClick={() => {
                void actions
                  .change({
                    action: "grant",
                    member_id: grant.user_id,
                    permissions: 0,
                    only_if_idle: true,
                  })
                  .then((outcome) => {
                    if (outcome !== "refused") setRemoving(false);
                  });
              }}
            >
              {copy.remove}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </fieldset>
  );
}
