"use client";

import { useCallback, useEffect, useRef, useState } from "react";

import { usePromptInputController } from "@/components/ai-elements/prompt-input";
import type { FileInMessage } from "@/core/messages/utils";

import { useAgentFileTransfers } from "./file-transfers";

type SavedConversationFile = FileInMessage & {
  path: string;
  status: "uploaded";
};
function savedReference(
  path: string | undefined,
  size: number | undefined,
): SavedConversationFile | null {
  if (
    !path?.startsWith("/mnt/user-data/uploads/") ||
    !Number.isSafeInteger(size) ||
    size! < 0
  )
    return null;
  const filename = path.split("/").at(-1);
  return filename &&
    !/[\\/\p{Cc}]/u.test(filename) &&
    ![".", ".."].includes(filename) &&
    path === `/mnt/user-data/uploads/${filename}`
    ? { path, filename, size: size!, status: "uploaded" }
    : null;
}

/** Upload parts once on attachment; only saved references cross the send boundary. */
export function useConversationAttachments({
  instanceId,
  threadId,
  enabled,
  changed,
}: {
  instanceId: string | null;
  threadId: string;
  enabled: boolean;
  changed: () => void;
}) {
  const { attachments } = usePromptInputController();
  const transfers = useAgentFileTransfers({
    instanceId: instanceId ?? "",
    threadId,
    folder: "uploads",
    enabled: enabled && !!instanceId,
    changed,
  });
  const scope = `${instanceId}:${threadId}`;
  const mapping = useRef<{ scope: string; parts: Map<string, string> }>({
    scope,
    parts: new Map(),
  });
  const [restored, setRestored] = useState<{
    scope: string;
    files: SavedConversationFile[];
  }>({ scope, files: [] });
  const current = useRef({
    scope,
    attachments,
    transfers,
    enabled,
    instanceId,
  });
  current.current = { scope, attachments, transfers, enabled, instanceId };
  useEffect(() => {
    if (mapping.current.scope !== scope)
      mapping.current = { scope, parts: new Map() };
    const owned = mapping.current.parts;
    const present = new Set(attachments.files.map((part) => part.id));
    for (const [part, operation] of owned) {
      if (!present.has(part)) {
        transfers.remove(operation);
        owned.delete(part);
      }
    }
    if (!enabled || !instanceId) return;
    for (const part of attachments.files) {
      if (owned.has(part.id) || !(part.file instanceof File)) continue;
      const [id] = transfers.add([part.file]);
      if (id) owned.set(part.id, id);
    }
  }, [scope, enabled, instanceId, attachments.files, transfers]);
  const liveFiles = transfers.rows.flatMap((row) => {
    const reference =
      row.status === "saved"
        ? savedReference(row.result?.sandbox_path, row.result?.size)
        : null;
    return reference ? [reference] : [];
  });
  const saved = [
    ...new Map(
      [...liveFiles, ...(restored.scope === scope ? restored.files : [])].map(
        (file) => [file.path, file],
      ),
    ).values(),
  ];
  const restoreFiles = useCallback(
    (files: FileInMessage[]) => {
      if (current.current.scope !== scope || !current.current.instanceId)
        return;
      setRestored((previous) => ({
        scope,
        files: [
          ...new Map(
            [
              ...(previous.scope === scope ? previous.files : []),
              ...files,
            ].flatMap((file) => {
              const checked = savedReference(file.path, file.size);
              return checked ? [[checked.path, checked] as const] : [];
            }),
          ).values(),
        ],
      }));
    },
    [scope],
  );
  const remove = (operation: string) => {
    const path = transfers.rows.find((row) => row.id === operation)?.result
      ?.sandbox_path;
    if (path)
      setRestored((previous) =>
        previous.scope !== scope
          ? previous
          : {
              scope,
              files: previous.files.filter((file) => file.path !== path),
            },
      );
    for (const [part, id] of mapping.current.parts)
      if (id === operation) attachments.remove(part);
    transfers.remove(operation);
  };
  const uploading =
    transfers.rows.some((row) => row.status === "uploading") ||
    (!!instanceId &&
      enabled &&
      attachments.files.some(
        (part) =>
          part.file instanceof File &&
          (mapping.current.scope !== scope ||
            !mapping.current.parts.has(part.id)),
      ));
  function capture() {
    const paths = new Set(
      restored.scope === scope ? restored.files.map((file) => file.path) : [],
    );
    return {
      files: saved,
      admitted: () => {
        if (current.current.scope !== scope) return;
        setRestored((previous) =>
          previous.scope !== scope
            ? previous
            : {
                scope,
                files: previous.files.filter((file) => !paths.has(file.path)),
              },
        );
      },
    };
  }
  return {
    rows: transfers.rows,
    saved,
    restored:
      restored.scope === scope
        ? restored.files.filter(
            (file) => !liveFiles.some((live) => live.path === file.path),
          )
        : [],
    uploading,
    capture,
    restoreFiles,
    remove,
    retry: transfers.retry,
    removeRestored: (path: string) =>
      setRestored((previous) =>
        previous.scope !== scope
          ? previous
          : {
              scope,
              files: previous.files.filter((file) => file.path !== path),
            },
      ),
  };
}
