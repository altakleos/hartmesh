"use client";

import type { AgentInstance } from "@/core/agent-instances/api";
import { useI18n } from "@/core/i18n/hooks";
import { pictureOf } from "@/core/telegram-bots/picture";
import { cn } from "@/lib/utils";

export function AgentAvatar({
  instance,
  className,
}: {
  instance: Pick<AgentInstance, "id" | "name" | "custody">;
  className?: string;
}) {
  const { t } = useI18n();
  const { initials, color } = pictureOf(instance.name, instance.id);
  const label =
    instance.custody === "personal"
      ? t.agentInstances.marker.aiAssistant
      : t.agentInstances.marker.aiEmployee;
  return (
    <span
      className={cn(
        "relative inline-flex size-10 shrink-0 items-center justify-center rounded-full text-sm font-semibold text-white",
        className,
      )}
      style={{ backgroundColor: color }}
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
