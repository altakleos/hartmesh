import { useI18n } from "@/core/i18n/hooks";

/** Lists may name an agent without exposing its kind or full detail record. */
export function AIBadge() {
  const { t } = useI18n();
  return (
    <span
      role="img"
      aria-label={t.agentInstances.marker.ai}
      className="bg-foreground text-background inline-block rounded px-1 text-[10px] leading-4"
    >
      {t.agentInstances.marker.ai}
    </span>
  );
}
