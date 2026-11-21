"use client";

import { useQueryClient } from "@tanstack/react-query";
import { useLayoutEffect, useRef, useState } from "react";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Switch } from "@/components/ui/switch";
import type { AgentInstance } from "@/core/agent-instances/api";
import {
  activeConnectionNames,
  isManaged,
  setConnection,
  useInstanceConnections,
  type ManagedConnection,
} from "@/core/agent-instances/connections";
import { hasCeiling, MANAGE } from "@/core/agent-instances/presentation";
import { useAuth } from "@/core/auth/AuthProvider";
import { useI18n } from "@/core/i18n/hooks";
import { usePeople } from "@/core/people/hooks";

type Instance = Pick<
  AgentInstance,
  "id" | "name" | "generation" | "permissions" | "status"
>;
export function ConnectionDisclosure({ instance }: { instance: Instance }) {
  const { t } = useI18n();
  const { data } = useInstanceConnections(instance, true);
  const names = activeConnectionNames(data?.connections ?? []);
  const [open, setOpen] = useState(false);
  if (!names.length) return null;
  const copy = t.agentInstances.connections;
  return (
    <>
      <button
        type="button"
        className="block max-w-full truncate text-left text-xs underline underline-offset-2"
        onClick={() => setOpen(true)}
      >
        {copy.active(instance.name, names.join(", "))}
      </button>
      <Dialog open={open} onOpenChange={setOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{copy.heading}</DialogTitle>
            <DialogDescription>
              {copy.active(instance.name, names.join(", "))}
            </DialogDescription>
          </DialogHeader>
          <p>{copy.disclosure(instance.name)}</p>
        </DialogContent>
      </Dialog>
    </>
  );
}
export function ActiveChatAccessNote({
  instance,
  person,
}: {
  instance: Instance;
  person: string;
}) {
  const { t } = useI18n();
  const { data } = useInstanceConnections(instance);
  return activeConnectionNames(data?.connections ?? []).length
    ? t.agentInstances.connections.chatOnly(person, instance.name)
    : null;
}
export function ManagerConnections({
  instance,
  disabled = false,
}: {
  instance: Instance;
  disabled?: boolean;
}) {
  const access = useInstanceConnections(instance);
  return (
    <ConnectionControls
      key={access.scope}
      instance={instance}
      disabled={disabled}
      access={access}
    />
  );
}
function ConnectionControls({
  instance,
  disabled,
  access,
}: {
  instance: Instance;
  disabled: boolean;
  access: ReturnType<typeof useInstanceConnections>;
}) {
  const { t, locale } = useI18n();
  const copy = t.agentInstances.connections;
  const { user } = useAuth();
  const client = useQueryClient();
  const items = access.data?.connections.filter(isManaged) ?? [];
  const people = usePeople(
    items.flatMap(({ given_by }) =>
      given_by && given_by !== user?.id ? [given_by] : [],
    ),
  );
  const [choice, setChoice] = useState<{
    row: ManagedConnection;
    count: number;
  } | null>(null);
  const [changing, setChanging] = useState<string | null>(null);
  const [error, setError] = useState(false);
  const owner = useRef<AbortController | null>(null);
  useLayoutEffect(() => () => owner.current?.abort(), []);
  const count = access.data?.chat_member_count;
  const canWrite =
    !disabled &&
    instance.status !== "deleted" &&
    !!user &&
    hasCeiling(user.permissions, "agents:write") &&
    !!(instance.permissions & MANAGE);
  const selected = choice && items.find((row) => row.id === choice.row.id);
  const unchanged =
    !!selected &&
    JSON.stringify(selected) === JSON.stringify(choice?.row) &&
    count === choice?.count;
  async function change(row: ManagedConnection, give: boolean) {
    if (!canWrite || owner.current || (give && (!row.can_give || !unchanged)))
      return;
    const request = new AbortController();
    owner.current = request;
    setChanging(row.id);
    setError(false);
    try {
      await setConnection(instance.id, row.id, give, request.signal);
    } catch {
      if (!request.signal.aborted) setError(true);
    } finally {
      if (!request.signal.aborted) {
        setChoice(null);
        await Promise.all([
          access.query.refetch(),
          client.invalidateQueries({ queryKey: ["mcpConfig"] }),
        ]);
        if (!request.signal.aborted) {
          setChanging(null);
          owner.current = null;
        }
      }
    }
  }
  return (
    <section className="space-y-3" aria-label={copy.heading}>
      <h3 className="text-lg font-semibold">{copy.heading}</h3>
      <p>{copy.intro(instance.name)}</p>
      {access.query.isPending ? (
        <p role="status">{t.common.loading}</p>
      ) : access.query.isError ? (
        <p role="alert">
          {copy.loadFailed}{" "}
          <Button variant="outline" onClick={() => void access.query.refetch()}>
            {t.agentInstances.refresh}
          </Button>
        </p>
      ) : !items.length ? (
        <p>{copy.empty}</p>
      ) : (
        <>
          <p className="text-muted-foreground text-sm">{copy.adminControl}</p>
          {items.some((row) => row.given) && (
            <p>{copy.exposure(instance.name)}</p>
          )}
          <ul className="space-y-2">
            {items.map((row) => {
              const date = row.given_at ? new Date(row.given_at) : null;
              const dateLabel =
                date && Number.isFinite(date.valueOf())
                  ? new Intl.DateTimeFormat(locale, {
                      dateStyle: "medium",
                    }).format(date)
                  : null;
              const person = row.given_by
                ? people.get(row.given_by)?.person
                : null;
              const by =
                row.given_by && row.given_by === user?.id
                  ? t.people.youInline
                  : person?.active
                    ? person.name?.trim()
                      ? person.name.trim()
                      : person.email
                    : null;
              return (
                <li key={row.id} className="space-y-1 rounded-lg border p-3">
                  <div className="flex items-center justify-between gap-2">
                    <span className="font-medium">{row.name}</span>
                    <Switch
                      aria-label={row.name}
                      checked={row.given}
                      disabled={
                        !canWrite ||
                        changing !== null ||
                        (!row.given &&
                          (!row.can_give ||
                            !Number.isSafeInteger(count) ||
                            (count ?? -1) < 0))
                      }
                      onCheckedChange={(give) => {
                        setError(false);
                        if (give && count !== undefined)
                          setChoice({ row, count });
                        else if (!give) void change(row, false);
                      }}
                    />
                  </div>
                  {row.host && (
                    <p className="text-muted-foreground text-xs">{row.host}</p>
                  )}
                  {row.description && <p>{row.description}</p>}
                  {row.given && dateLabel && (
                    <p>
                      {by
                        ? copy.givenBy(by, dateLabel)
                        : copy.givenOn(dateLabel)}
                    </p>
                  )}
                  {row.reason && (
                    <p>
                      {Object.hasOwn(copy.reasons, row.reason)
                        ? copy.reasons[row.reason]
                        : copy.unsupported}
                    </p>
                  )}
                </li>
              );
            })}
          </ul>
        </>
      )}
      {error && <p role="alert">{copy.writeFailed}</p>}
      <Dialog
        open={choice !== null}
        onOpenChange={(open) => {
          if (!open && !changing) setChoice(null);
        }}
      >
        <DialogContent className="max-h-[85dvh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle>
              {copy.giveTitle(instance.name, choice?.row.name ?? "")}
            </DialogTitle>
            <DialogDescription>
              {copy.giveScope(instance.name)}
            </DialogDescription>
          </DialogHeader>
          <p>{copy.giveData(instance.name)}</p>
          <p>{copy.giveAccount}</p>
          <p>{copy.brief(instance.name)}</p>
          {choice && <p>{copy.members(choice.count, instance.name)}</p>}
          {!unchanged && <p role="alert">{copy.changed}</p>}
          <DialogFooter>
            <Button
              variant="outline"
              disabled={changing !== null}
              onClick={() => setChoice(null)}
            >
              {t.common.cancel}
            </Button>
            <Button
              disabled={!canWrite || !unchanged || changing !== null}
              onClick={() => {
                if (selected) void change(selected, true);
              }}
            >
              {copy.give}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </section>
  );
}
