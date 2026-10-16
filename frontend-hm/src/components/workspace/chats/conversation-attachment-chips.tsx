"use client";

import { Button } from "@/components/ui/button";
import { AgentTransferRow } from "@/components/workspace/instances/agent-file-transfer";
import type { AgentInstanceDetail } from "@/core/agent-instances/api";
import type { useConversationAttachments } from "@/core/agent-instances/conversation-attachments";
import { INSPECT, MANAGE } from "@/core/agent-instances/presentation";
import { useI18n } from "@/core/i18n/hooks";
import type { AgentThread } from "@/core/threads/types";

export function ConversationAttachmentChips({
  binding,
  detail,
  files,
  disabled,
}: {
  binding: NonNullable<AgentThread["agent_instance"]>;
  detail: AgentInstanceDetail | null;
  files: ReturnType<typeof useConversationAttachments>;
  disabled: boolean;
}) {
  const { t } = useI18n();
  return (
    <>
      {files.rows.map((row) => (
        <AgentTransferRow
          key={row.id}
          conversation
          instance={detail ?? binding}
          row={row}
          manager={
            !!detail &&
            (detail.permissions & (MANAGE | INSPECT)) === (MANAGE | INSPECT)
          }
          disabled={disabled}
          retry={() => files.retry(row.id)}
          remove={() => files.remove(row.id)}
        />
      ))}
      {files.restored.map((file) => (
        <article key={file.path} className="space-y-2 rounded border p-3">
          <p>{file.filename}</p>
          <p role="status">{t.inputBox.agentFile.saved(binding.name)}</p>
          <Button
            type="button"
            variant="outline"
            onClick={() => files.removeRestored(file.path)}
          >
            {t.agentInstances.remove}
          </Button>
        </article>
      ))}
    </>
  );
}
