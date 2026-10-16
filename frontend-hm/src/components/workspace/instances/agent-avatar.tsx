"use client";

import type { AgentInstance } from "@/core/agent-instances/api";
import { useI18n } from "@/core/i18n/hooks";
import { cn } from "@/lib/utils";

const colours = [
  "bg-blue-100 text-blue-900 dark:bg-blue-950 dark:text-blue-100",
  "bg-violet-100 text-violet-900 dark:bg-violet-950 dark:text-violet-100",
  "bg-emerald-100 text-emerald-900 dark:bg-emerald-950 dark:text-emerald-100",
  "bg-amber-100 text-amber-900 dark:bg-amber-950 dark:text-amber-100",
  "bg-rose-100 text-rose-900 dark:bg-rose-950 dark:text-rose-100",
  "bg-cyan-100 text-cyan-900 dark:bg-cyan-950 dark:text-cyan-100",
];

export function AgentAvatar({
  instance,
  className,
}: {
  instance: Pick<AgentInstance, "id" | "name" | "custody">;
  className?: string;
}) {
  const { t } = useI18n();
  let colour = 0;
  for (const character of instance.id)
    colour = (colour * 31 + character.codePointAt(0)!) >>> 0;
  const words = instance.name.trim().split(/\s+/).filter(Boolean);
  const initials = words
    .slice(0, 2)
    .map((word) => Array.from(word)[0])
    .join("")
    .toLocaleUpperCase();
  const label =
    instance.custody === "personal"
      ? t.agentInstances.marker.aiAssistant
      : t.agentInstances.marker.aiEmployee;
  return (
    <span
      className={cn(
        "relative inline-flex size-10 shrink-0 items-center justify-center rounded-full text-sm font-semibold",
        colours[colour % colours.length],
        className,
      )}
    >
      <span aria-hidden>{initials}</span>
      <span
        role="img"
        aria-label={label}
        title={label}
        className="bg-foreground text-background absolute -right-1 -bottom-1 rounded px-1 text-[10px] leading-4"
      >
        {t.agentInstances.marker.ai}
      </span>
    </span>
  );
}
