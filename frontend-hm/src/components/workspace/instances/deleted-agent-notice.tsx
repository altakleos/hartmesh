"use client";

import { useQueryClient } from "@tanstack/react-query";
import { useSearchParams } from "next/navigation";
import { useEffect } from "react";

import type { Custody } from "@/core/agent-instances/api";
import { useI18n } from "@/core/i18n/hooks";

export function DeletedAgentNotice({ kind }: { kind: Custody }) {
  const search = useSearchParams();
  const client = useQueryClient();
  const { t } = useI18n();
  const value = search.get("deleted");
  const name = value && Array.from(value).length <= 128 ? value : null;
  const clearing = search.get("clearing") === "1";
  useEffect(() => {
    if (!name) return;
    void client.invalidateQueries({ queryKey: ["features"] });
    if (!clearing) return;
    let polls = 0;
    const timer = setInterval(() => {
      void client.invalidateQueries({ queryKey: ["features"] });
      if (++polls >= 12) clearInterval(timer);
    }, 15_000);
    return () => clearInterval(timer);
  }, [name, clearing, client]);
  return name ? (
    <p role="status">
      {clearing
        ? t.agentInstances.settings.deletedClearing(name, kind)
        : t.agentInstances.settings.deleted(name)}
    </p>
  ) : null;
}
