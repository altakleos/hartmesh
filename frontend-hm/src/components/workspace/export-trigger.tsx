"use client";

import { Download, FileJson, FileText } from "lucide-react";
import { useCallback } from "react";
import { toast } from "sonner";

import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { useI18n } from "@/core/i18n/hooks";
import { isStaticWebsiteOnly } from "@/core/static-mode";
import {
  exportThread,
  ThreadExportEmptyError,
  type ThreadExportFormat,
} from "@/core/threads/export";

import { useThread } from "./messages/context";
import { Tooltip } from "./tooltip";

export function ExportTrigger({ threadId }: { threadId: string }) {
  const { t } = useI18n();
  const { thread } = useThread();

  const messages = thread.messages;

  const handleExport = useCallback(
    async (format: ThreadExportFormat) => {
      if (messages.length === 0) {
        toast.error(t.conversation.noMessages);
        return;
      }
      try {
        await exportThread(threadId, format);
        toast.success(t.common.exportSuccess);
      } catch (error) {
        toast.error(
          error instanceof ThreadExportEmptyError
            ? t.conversation.noMessages
            : t.common.exportFailed,
        );
      }
    },
    [messages.length, threadId, t],
  );

  // The Gateway writes the transcript; a static demo has none to ask.
  if (messages.length === 0 || isStaticWebsiteOnly()) {
    return null;
  }

  return (
    <DropdownMenu>
      <Tooltip content={t.common.export}>
        <DropdownMenuTrigger asChild>
          <Button
            aria-label={t.common.export}
            className="text-muted-foreground hover:text-foreground"
            variant="ghost"
          >
            <Download />
            <span className="hidden sm:inline">{t.common.export}</span>
          </Button>
        </DropdownMenuTrigger>
      </Tooltip>
      <DropdownMenuContent align="end">
        <DropdownMenuItem onSelect={() => void handleExport("markdown")}>
          <FileText className="text-muted-foreground" />
          <span>{t.common.exportAsMarkdown}</span>
        </DropdownMenuItem>
        <DropdownMenuItem onSelect={() => void handleExport("json")}>
          <FileJson className="text-muted-foreground" />
          <span>{t.common.exportAsJSON}</span>
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
