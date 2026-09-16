"use client";

import { FileWarningIcon } from "lucide-react";
import { useId } from "react";

import { useArtifactDeliveryFailure } from "@/core/artifact-delivery";
import { useI18n } from "@/core/i18n/hooks";
import { cn } from "@/lib/utils";

import { ArtifactFileList } from "../artifacts/artifact-file-list";

/**
 * The turn produced files and ended without presenting any of them.
 *
 * Rendered under the run's own last assistant bubble, beside the workspace
 * changes card, so the correction sits where the confident prose is rather
 * than in a toast that has already gone. The files themselves are offered
 * through the ordinary artifact list, so opening and downloading them works
 * exactly as it would have if the assistant had presented them.
 */
export function UndeliveredFilesNotice({
  className,
  runId,
  threadId,
}: {
  className?: string;
  runId?: string;
  threadId: string;
}) {
  const { t } = useI18n();
  const titleId = useId();
  const failure = useArtifactDeliveryFailure(runId);

  if (!failure) {
    return null;
  }

  const shown = failure.undeliveredPaths.length;
  const truncated = failure.undeliveredCount > shown;

  return (
    <div
      aria-labelledby={titleId}
      className={cn(
        // Amber border and glyph on the ordinary neutral surface: enough to
        // separate a correction from the workspace-changes summary stacked
        // below it, not enough to read as an alert. This card is now the only
        // signal — there is no toast — so it must be findable without being
        // alarming.
        "bg-muted/20 mt-3 overflow-hidden rounded-xl border border-amber-500/40",
        className,
      )}
      data-testid="undelivered-files-notice"
      role="group"
    >
      <div className="flex items-start gap-2.5 p-3">
        <div className="bg-background/80 flex size-10 shrink-0 items-center justify-center rounded-lg">
          <FileWarningIcon className="size-4 text-amber-600 dark:text-amber-500" />
        </div>
        <div className="min-w-0">
          <div className="text-foreground text-sm font-semibold" id={titleId}>
            {t.artifactDelivery.title(failure.undeliveredCount)}
          </div>
          <p className="text-muted-foreground mt-0.5 text-xs">
            {t.artifactDelivery.description(failure.undeliveredCount)}
            {truncated
              ? ` ${t.artifactDelivery.shownOfTotal(shown, failure.undeliveredCount)}`
              : ""}
          </p>
        </div>
      </div>
      <ArtifactFileList
        archiveDownloadsEnabled={false}
        className="px-3 pb-3"
        files={failure.undeliveredPaths}
        skillInstallEnabled={false}
        threadId={threadId}
      />
    </div>
  );
}
