import type { FeaturesResponse } from "@/core/features/api";
import type { ImportReceipt } from "@/core/threads/conversation-file-import";

export type SharingOperation =
  | "read"
  | "publish"
  | "continue"
  | "agent_publish";
const operations: SharingOperation[] = [
  "read",
  "publish",
  "continue",
  "agent_publish",
];
export function sharedConversationOperations(
  features: Pick<FeaturesResponse, "shared_conversations">,
) {
  const available = features.shared_conversations;
  return available?.version === 1 && Array.isArray(available.operations)
    ? operations.filter((op) => available.operations.includes(op))
    : [];
}

export interface SnapshotFile {
  path: string;
  size: number;
  sha256: string;
  also_named?: string[];
}
export interface SharedMember extends SnapshotFile {
  available: boolean;
  publication_id: string | null;
  shared_path: string | null;
}
export interface SnapshotSummary {
  publication_id: string;
  title: string;
  publisher: string | null;
  published_at: string;
}
export interface Snapshot extends SnapshotSummary {
  with: { kind: "employee" | "assistant"; id: string; name: string } | null;
  messages: { type: "ai" | "human"; content: string }[];
  transcript: SharedMember;
  files: SharedMember[];
  can_remove: boolean;
  answerer: { kind: "assistant" };
}
export interface RetiredSnapshot {
  removed: true;
  publisher: string | null;
  published_at: string;
}
export type SnapshotOrigin = NonNullable<Snapshot["with"]>;
export interface SnapshotAudience {
  starter: string | null;
  starter_can_read: boolean;
  authors: string[];
  inspect_count: number;
  chat_count: number;
}

export interface SnapshotDraft {
  with?: SnapshotOrigin;
  audience?: SnapshotAudience;
  title: string;
  snapshot: string;
  message_count: number;
  files: SnapshotFile[];
  already_shared: boolean;
  publication_id: string | null;
}
export function reviewedAgentAudience(
  draft: SnapshotDraft | undefined,
  instanceId: string,
) {
  const a = draft?.audience;
  if (
    !a ||
    draft?.with?.id !== instanceId ||
    !["assistant", "employee"].includes(draft.with.kind) ||
    typeof draft.with.name !== "string" ||
    !draft.with.name.trim() ||
    !/^[0-9a-f]{64}$/.test(draft.snapshot) ||
    !(a.starter === null || typeof a.starter === "string") ||
    typeof a.starter_can_read !== "boolean" ||
    !Array.isArray(a.authors) ||
    !a.authors.every((n) => typeof n === "string") ||
    !Number.isSafeInteger(a.inspect_count) ||
    a.inspect_count < 0 ||
    !Number.isSafeInteger(a.chat_count) ||
    a.chat_count < 0
  )
    return null;
  return a;
}
export interface PublishResult {
  publication_id: string;
  created_publication_ids: string[];
  undo_receipt: string | null;
  already_shared: boolean;
}
export interface ContinueResult {
  thread_id: string;
  answerer: { kind: "assistant" };
  files: (
    | { path: string; status: "copied"; attachment: ImportReceipt }
    | { path: string; status: "not_copied"; reason: string }
  )[];
}

export function ordinarySharedRows<
  T extends { path: string; snapshot_publication_id?: string | null },
>(rows: readonly T[], recognized: boolean) {
  return recognized ? rows.filter((row) => !row.snapshot_publication_id) : rows;
}
export interface SharedHistory {
  publication_id: string;
  message_ids: string[];
}
type SeedMessage = { id?: string; additional_kwargs?: Record<string, unknown> };
export function readSharedHistory(
  messages: readonly SeedMessage[],
  proof: SharedHistory | null | undefined,
) {
  const copied = new Set<string>();
  if (!proof?.publication_id || !proof.message_ids.length)
    return { copied, last: null };
  const ids = new Set(proof.message_ids);
  for (const message of messages) {
    const mark = message.additional_kwargs?.shared_history;
    if (
      message.id &&
      ids.has(message.id) &&
      mark &&
      typeof mark === "object" &&
      "publication_id" in mark &&
      mark.publication_id === proof.publication_id
    )
      copied.add(message.id);
  }
  const last = proof.message_ids.at(-1)!;
  return { copied, last: copied.has(last) ? last : null };
}
