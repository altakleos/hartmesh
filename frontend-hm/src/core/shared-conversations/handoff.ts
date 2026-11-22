import {
  useQuery,
  useQueryClient,
  type QueryClient,
} from "@tanstack/react-query";

import { getSessionComposerDraftStorage } from "@/core/threads/composer-draft";
import {
  importedAttachment,
  type ImportReceipt,
} from "@/core/threads/conversation-file-import";

import type { ContinueResult } from "./protocol";

export interface ContinuedDraft {
  publication: string;
  request: string;
  title: string;
  files: { source: string; receipt: ImportReceipt }[];
  missing: { path: string; reason: string }[];
}
const key = (account: string, thread: string) =>
  ["shared-conversation-draft", account, thread] as const;
const storageKey = (account: string, thread: string) =>
  JSON.stringify(key(account, thread));
function read(account: string, thread: string): ContinuedDraft | null {
  try {
    const raw = getSessionComposerDraftStorage()?.getItem(
      storageKey(account, thread),
    );
    if (!raw) return null;
    const d = JSON.parse(raw) as ContinuedDraft;
    if (
      typeof d.publication !== "string" ||
      typeof d.request !== "string" ||
      typeof d.title !== "string" ||
      !Array.isArray(d.files) ||
      !Array.isArray(d.missing)
    )
      return null;
    d.files.forEach((f) => importedAttachment(f.receipt));
    if (
      d.missing.some(
        (f) => typeof f.path !== "string" || typeof f.reason !== "string",
      )
    )
      return null;
    return d;
  } catch {
    return null;
  }
}
function store(
  client: QueryClient,
  account: string,
  thread: string,
  d: ContinuedDraft | null,
) {
  client.setQueryData(key(account, thread), d);
  try {
    const storage = getSessionComposerDraftStorage(),
      k = storageKey(account, thread);
    if (d) storage?.setItem(k, JSON.stringify(d));
    else storage?.removeItem(k);
  } catch {
    /* The tab retains the in-memory draft when browser storage is full. */
  }
}
export function saveContinuedDraft(
  client: QueryClient,
  account: string,
  publication: string,
  request: string,
  title: string,
  result: ContinueResult,
  previous?: ContinuedDraft,
) {
  const files = previous?.files.slice() ?? [];
  const wanted = previous ? new Set(previous.missing.map((f) => f.path)) : null;
  const missing: ContinuedDraft["missing"] = [];
  for (const f of result.files) {
    if (wanted && !wanted.has(f.path)) continue;
    if (f.status === "not_copied") {
      missing.push({ path: f.path, reason: f.reason });
      continue;
    }
    importedAttachment(f.attachment);
    if (
      !files.some(
        (existing) =>
          existing.receipt.virtual_path === f.attachment.virtual_path,
      )
    )
      files.push({ source: f.path, receipt: f.attachment });
  }
  store(
    client,
    account,
    result.thread_id,
    files.length || missing.length
      ? { publication, request, title, files, missing }
      : null,
  );
}
export function useContinuedDraft(account: string | undefined, thread: string) {
  const client = useQueryClient();
  const { data } = useQuery({
    queryKey: key(account ?? "", thread),
    queryFn: () => (account ? read(account, thread) : null),
    enabled: !!account,
    staleTime: Infinity,
    retry: false,
  });
  const draft = account ? data : null;
  const chips = (draft?.files ?? []).map((f) => ({
    file: importedAttachment(f.receipt),
    sourceTitle: draft!.title,
  }));
  function remove(paths: Set<string>) {
    if (!account || !draft || !paths.size) return;
    const current = client.getQueryData<ContinuedDraft | null>(
      key(account, thread),
    );
    if (current?.request !== draft.request) return;
    const files = current.files.filter(
      (f) => !paths.has(f.receipt.virtual_path),
    );
    store(
      client,
      account,
      thread,
      files.length || current.missing.length ? { ...current, files } : null,
    );
  }
  return { draft, chips, remove };
}
