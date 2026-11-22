"use client";
import { useInfiniteQuery } from "@tanstack/react-query";
import Link from "next/link";
import { useState } from "react";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import * as api from "@/core/agent-instances/api";
import { hasCeiling, INSPECT } from "@/core/agent-instances/presentation";
import { useAuth } from "@/core/auth/AuthProvider";
import { useAgentInstances } from "@/core/features";
import { browserHref, type BrowserArea } from "@/core/file-areas/browser";
import { useI18n } from "@/core/i18n/hooks";

import { useBrowserCopy } from "./copy";

function AgentLocations({
  kind,
  filter,
  current,
}: {
  kind: api.Custody;
  filter: string;
  current?: string;
}) {
  const { user } = useAuth(),
    feature = useAgentInstances(),
    { t, locale } = useI18n(),
    copy = useBrowserCopy();
  const allowed =
    !!user &&
    hasCeiling(user.permissions, "agents:read") &&
    feature.known &&
    feature.enabled;
  const list = useInfiniteQuery({
    queryKey: [
      "agent-instances",
      user?.id,
      user?.system_role,
      user?.permissions,
      kind,
    ],
    enabled: allowed,
    initialPageParam: 0,
    queryFn: ({ pageParam, signal }) =>
      api.listInstances(kind, pageParam, signal),
    getNextPageParam: (page, _pages, offset) =>
      page.instances.length === 100 ? offset + 100 : undefined,
    retry: false,
  });
  if (!allowed) return null;
  const group =
    kind === "personal"
      ? t.agentInstances.assistants.title
      : t.agentInstances.employees.title;
  const entries = list.isError
    ? []
    : (list.data?.pages
        .flatMap((p) => p.instances)
        .filter(api.isFullInstance)
        .filter(
          (a) =>
            !!a.home_id &&
            !!(a.permissions & INSPECT) &&
            !["deleted", "provisioning"].includes(a.status),
        ) ?? []);
  const counts = new Map<string, number>();
  for (const entry of entries)
    counts.set(entry.name, (counts.get(entry.name) ?? 0) + 1);
  const normalize = (s: string) =>
    s.normalize("NFKC").toLocaleLowerCase(locale);
  return (
    <section aria-label={group}>
      <h3 className="text-muted-foreground text-xs font-medium">{group}</h3>
      {entries
        .filter((a) => normalize(a.name).includes(normalize(filter)))
        .map((a) => (
          <Link
            className="block min-h-11 py-2 break-words underline"
            key={a.id}
            aria-current={current === a.id ? "location" : undefined}
            href={browserHref("agent", "", {
              instance: a.id,
            })}
          >
            {a.name}
            {(counts.get(a.name) ?? 0) > 1 && (
              <small className="ml-2">{a.id}</small>
            )}
          </Link>
        ))}
      {list.hasNextPage && (
        <Button
          type="button"
          variant="outline"
          disabled={list.isFetchingNextPage}
          onClick={() => void list.fetchNextPage()}
        >
          {copy.more} · {group}
        </Button>
      )}
      {list.isError && (
        <Button variant="outline" onClick={() => void list.refetch()}>
          {t.files.retry}
        </Button>
      )}
    </section>
  );
}

export function BrowserLocations({
  area,
  instance,
}: {
  area: BrowserArea;
  instance?: string;
}) {
  const copy = useBrowserCopy(),
    { user } = useAuth();
  const [open, setOpen] = useState(false),
    [filter, setFilter] = useState("");
  return (
    <Dialog
      open={open}
      onOpenChange={(v) => {
        setOpen(v);
        if (!v) setFilter("");
      }}
    >
      <DialogTrigger asChild>
        <Button className="min-h-11" variant="outline">
          {copy.locations}
        </Button>
      </DialogTrigger>
      <DialogContent
        className="max-h-[85dvh] overflow-y-auto"
        aria-describedby={undefined}
      >
        <DialogTitle>{copy.locations}</DialogTitle>
        {user &&
          hasCeiling(user.permissions, "threads:read") &&
          (["personal", "shared"] as const).map((a) => (
            <Link
              className="aria-current:bg-muted block min-h-11 rounded-md px-2 py-2"
              key={a}
              aria-current={a === area ? "location" : undefined}
              href={browserHref(a, "")}
              onClick={() => setOpen(false)}
            >
              {copy[a]}
            </Link>
          ))}
        <Input
          aria-label={copy.locationsFilter}
          placeholder={copy.locationsFilter}
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
        />
        {open &&
          (["personal", "company"] as const).map((kind) => (
            <AgentLocations
              key={kind}
              kind={kind}
              current={instance}
              filter={filter}
            />
          ))}
      </DialogContent>
    </Dialog>
  );
}
