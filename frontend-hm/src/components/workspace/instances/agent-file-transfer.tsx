"use client";
import { Button } from "@/components/ui/button";
import type { AgentInstance } from "@/core/agent-instances/api";
import type { AgentTransfer } from "@/core/agent-instances/file-transfers";
import { useI18n } from "@/core/i18n/hooks";

import { AgentFileRefusal } from "./file-refusal";

export function AgentTransferRow({
  instance,
  row,
  retry,
  remove,
  disabled = false,
  conversation = false,
  manager = true,
}: {
  instance: Pick<AgentInstance, "name" | "custody"> & {
    supervisor?: AgentInstance["supervisor"];
  };
  row: AgentTransfer;
  retry: () => void;
  remove: () => void;
  disabled?: boolean;
  conversation?: boolean;
  manager?: boolean;
}) {
  const { t } = useI18n();
  const copy = conversation ? t.inputBox.agentFile : t.agentInstances.files;
  const savedName = row.result?.path.split("/").at(-1);
  return (
    <article className="space-y-2 rounded border p-3">
      <p className="font-medium">{row.file.name}</p>
      {row.status === "uploading" ? (
        <>
          <p role="status">{copy.uploading(Math.round(row.percentage))}</p>
          <Button type="button" variant="outline" onClick={remove}>
            {t.common.cancel}
          </Button>
        </>
      ) : row.status === "saved" ? (
        <>
          <p role="status">
            {savedName && savedName !== row.file.name
              ? copy.savedAs(savedName)
              : copy.saved(instance.name)}
          </p>
          {conversation && (
            <Button type="button" variant="outline" onClick={remove}>
              {t.agentInstances.remove}
            </Button>
          )}
        </>
      ) : (
        <>
          <AgentFileRefusal
            instance={instance}
            error={row.error}
            file={row.file.name}
            manager={manager}
            place={conversation ? "conversation" : "files"}
          />
          <div className="flex gap-2">
            {row.retryable && (
              <Button type="button" disabled={disabled} onClick={retry}>
                {t.agentInstances.retry}
              </Button>
            )}
            <Button type="button" variant="outline" onClick={remove}>
              {t.agentInstances.remove}
            </Button>
          </div>
        </>
      )}
    </article>
  );
}
