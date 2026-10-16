"use client";

import { useAuth } from "@/core/auth/AuthProvider";
import { useI18n } from "@/core/i18n/hooks";
import { usePeople } from "@/core/people/hooks";

export function usePersonNames(
  ids: readonly string[],
  inline = false,
): Map<string, string | null> {
  const { user } = useAuth();
  const { t } = useI18n();
  const people = usePeople(ids.filter((id) => !!id && id !== user?.id));
  return new Map(
    ids.map((id) => {
      if (id === user?.id)
        return [id, inline ? t.people.youInline : t.people.you];
      const result = people.get(id);
      if (!result || result.status === "loading") return [id, null];
      if (result.status === "error") return [id, t.people.unknown];
      if (!result.person) return [id, t.people.former];
      const trimmed = result.person.name?.trim();
      const name = trimmed?.length ? trimmed : result.person.email;
      return [id, result.person.active ? name : t.people.formerNamed(name)];
    }),
  );
}
export function usePersonName(id: string, inline = false): string | null {
  return usePersonNames([id], inline).get(id) ?? null;
}

/** Sentences asking for help must name someone the reader can actually ask. */
export function useSupervisorName(id: string): string {
  const { user } = useAuth();
  const { t } = useI18n();
  const people = usePeople(!id || id === user?.id ? [] : [id]);
  const result = people.get(id);
  if (id === user?.id || result?.status !== "ready" || !result.person?.active)
    return t.people.aManager;
  const trimmed = result.person.name?.trim();
  return trimmed?.length ? trimmed : result.person.email;
}

export function PersonName({
  id,
  inline = false,
}: {
  id: string;
  inline?: boolean;
}) {
  const name = usePersonName(id, inline);
  return <span aria-busy={name === null}>{name ?? "…"}</span>;
}
