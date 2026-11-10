import type {
  AgentInstance,
  AgentInstanceDetail,
} from "@/core/agent-instances/api";
import { USE } from "@/core/agent-instances/presentation";
import type {
  AgentInstancesFeature,
  ExecutionReason,
  TelegramFeature,
} from "@/core/features/api";

import type { BotList } from "./api";
import { sectionNoticeOf } from "./rows";
import type { TelegramKey } from "./text";

export type InstanceConnectBlock =
  | { kind: "telegram"; key: TelegramKey }
  | { kind: "execution"; reason: ExecutionReason | null }
  | {
      kind:
        | "policy"
        | "use"
        | "retired"
        | "personal-off"
        | "company-off"
        | "limit"
        | "loading"
        | "unreadable"
        | "ceiling";
    };

type Instance = AgentInstance & {
  execution?: AgentInstanceDetail["execution"];
};
/** Every entry point uses the same first applicable explanation. Busy files do not prevent connecting. */
export function instanceConnectBlock({
  instance,
  feature,
  telegram,
  bots,
  canRead,
}: {
  instance: Instance;
  feature: AgentInstancesFeature;
  telegram: TelegramFeature;
  bots: BotList | undefined;
  canRead: boolean;
}): InstanceConnectBlock | null {
  if (!telegram.known) return { kind: "loading" };
  const notice = sectionNoticeOf(telegram, (bots?.bots.length ?? 0) > 0);
  if (notice) return { kind: "telegram", key: notice.key };
  if (!bots) return { kind: "loading" };
  if (
    instance.custody === "company" &&
    bots.policies.some(
      (policy) => policy.instance_id === instance.id && policy.policy === "off",
    )
  )
    return { kind: "policy" };
  if (!canRead) return { kind: "ceiling" };
  if (
    !(instance.permissions & USE) ||
    instance.status === "deleted" ||
    instance.status === "provisioning"
  )
    return { kind: "use" };
  if (instance.status === "archived") return { kind: "retired" };
  if (!feature.known) return { kind: "execution", reason: null };
  if (
    feature.reason === "agents-none" ||
    (instance.custody === "personal"
      ? feature.assistants.perUser === 0
      : feature.employees.allowed === 0)
  )
    return {
      kind: instance.custody === "personal" ? "personal-off" : "company-off",
    };
  if (!feature.enabled || feature.execution !== "ready")
    return { kind: "execution", reason: feature.reason };
  if (instance.execution?.execution === "unavailable")
    return { kind: "execution", reason: instance.execution.reason };
  if (bots.limits.used >= bots.limits.per_person) return { kind: "limit" };
  return null;
}
