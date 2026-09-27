"use client";

import { CheckIcon, DownloadIcon, Loader2Icon } from "lucide-react";
import { useState, type ReactNode } from "react";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Progress } from "@/components/ui/progress";
import {
  AccountExportBusyError,
  AccountExportUnavailableError,
  accountExportPercent,
  urlOfAccountExportPart,
  useAccountExport,
  useDiscardAccountExport,
  useStartAccountExport,
  type AccountExportStatus,
} from "@/core/account-export";
import { useI18n } from "@/core/i18n/hooks";
import { formatUploadSize } from "@/core/uploads/file-validation";

type Translations = ReturnType<typeof useI18n>["t"]["accountExport"];

function messageFor(error: unknown, t: Translations): string {
  if (error instanceof AccountExportBusyError) return t.busy;
  if (error instanceof AccountExportUnavailableError) return t.unavailable;
  return t.failed;
}

/** A time the person reads: the hour alone today, with the date on another day. */
function whenLabel(value: string, locale: string): string {
  const when = new Date(value);
  const today = when.toDateString() === new Date().toDateString();
  return today
    ? when.toLocaleTimeString(locale, { timeStyle: "short" })
    : when.toLocaleString(locale, { dateStyle: "medium", timeStyle: "short" });
}

function Building({
  status,
  t,
}: {
  status: AccountExportStatus;
  t: Translations;
}) {
  const { progress } = status;
  const percent = accountExportPercent(progress);
  // Nothing is counted until the Gateway has found what to export.
  const counted = progress.conversations_total + progress.files_total > 0;
  return (
    <div className="flex flex-col gap-3">
      <p className="flex items-center gap-2 text-sm">
        <Loader2Icon className="size-4 animate-spin" aria-hidden />
        {t.preparing}
      </p>
      <Progress
        value={percent}
        aria-label={t.preparing}
        aria-valuetext={`${percent}%`}
      />
      {counted && (
        <p className="text-muted-foreground text-xs tabular-nums">
          {t.conversationsProgress(
            progress.conversations_done,
            progress.conversations_total,
          )}
          {progress.files_total > 0 &&
            ` · ${t.filesProgress(progress.files_done, progress.files_total)}`}
        </p>
      )}
      <p className="text-muted-foreground text-sm">{t.closeAnytime}</p>
    </div>
  );
}

function Ready({
  status,
  t,
  locale,
}: {
  status: AccountExportStatus;
  t: Translations;
  locale: string;
}) {
  const count = status.parts.length;
  return (
    <div className="flex flex-col gap-3">
      <p className="text-sm">
        {status.state === "downloaded" ? t.allDownloaded : t.ready}
      </p>
      <p className="text-muted-foreground text-sm">
        {t.includesUpTo(whenLabel(status.started_at, locale))}
      </p>
      {count > 1 && (
        <p className="text-muted-foreground text-sm">{t.multiPart}</p>
      )}
      <ul className="flex flex-col gap-2">
        {status.parts.map((part) => (
          <li key={part.number} className="flex flex-wrap items-center gap-3">
            <Button asChild variant={part.downloaded ? "outline" : "default"}>
              {/* A plain link: the browser saves it and shows its own progress, however large. */}
              <a href={urlOfAccountExportPart(part.number)} download>
                <DownloadIcon />
                {count > 1 ? t.downloadPart(part.number, count) : t.download}
              </a>
            </Button>
            <span className="text-muted-foreground text-xs tabular-nums">
              {formatUploadSize(part.size)}
            </span>
            {part.downloaded && (
              <span className="text-muted-foreground flex items-center gap-1 text-xs">
                <CheckIcon className="size-3" aria-hidden />
                {t.downloaded}
              </span>
            )}
          </li>
        ))}
      </ul>
      {status.skipped > 0 && (
        <p className="text-muted-foreground text-sm">
          {t.skipped(status.skipped)}
        </p>
      )}
      {status.state === "ready" && (
        <p className="text-muted-foreground text-xs">
          {status.expires_at === null
            ? t.keptWhileDownloading
            : t.availableUntil(whenLabel(status.expires_at, locale))}
        </p>
      )}
    </div>
  );
}

function Alert({ children }: { children: ReactNode }) {
  return (
    <p className="text-destructive text-sm" role="alert">
      {children}
    </p>
  );
}

/**
 * *Download all my data*: the person prepares one download of all of their
 * own work, follows it while the Gateway builds it, then downloads it. The
 * Gateway keeps the export, so closing the dialog or reloading the page
 * loses nothing: opening it again picks the same export up.
 */
export function AccountExportDialog({
  open,
  onOpenChange,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const { t: all, locale } = useI18n();
  const t = all.accountExport;
  const query = useAccountExport(open);
  const start = useStartAccountExport();
  const discard = useDiscardAccountExport();
  const [startError, setStartError] = useState<unknown>(null);
  const [deleteFailed, setDeleteFailed] = useState(false);
  // Whether this opening saw an export that has since gone without the person deleting it.
  const [seen, setSeen] = useState(false);
  const status = query.data;
  if (status && !seen) {
    setSeen(true);
  }

  const prepare = () => {
    setStartError(null);
    setSeen(false);
    start.mutate(undefined, { onError: (error) => setStartError(error) });
  };
  const remove = () => {
    setDeleteFailed(false);
    discard.mutate(undefined, {
      onSuccess: () => setSeen(false),
      onError: () => setDeleteFailed(true),
    });
  };

  let body: ReactNode = null;
  let footer: ReactNode = null;
  if (query.isError && !status) {
    // A poll that fails while an export is shown keeps showing it: the next one will do.
    body = (
      <Alert>
        {query.error instanceof AccountExportUnavailableError
          ? t.unavailable
          : t.loadFailed}
      </Alert>
    );
    if (!(query.error instanceof AccountExportUnavailableError)) {
      footer = (
        <Button variant="outline" onClick={() => void query.refetch()}>
          {t.tryAgain}
        </Button>
      );
    }
  } else if (query.isPending) {
    body = (
      <Loader2Icon
        className="size-4 animate-spin"
        role="status"
        aria-label={t.preparing}
      />
    );
  } else if (!status) {
    body = (
      <>
        {startError ? (
          <Alert>{messageFor(startError, t)}</Alert>
        ) : (
          seen && <p className="text-sm">{t.gone}</p>
        )}
        <p className="text-muted-foreground text-sm">{t.closeAnytime}</p>
      </>
    );
    footer = (
      <Button onClick={prepare} disabled={start.isPending}>
        {start.isPending && <Loader2Icon className="animate-spin" />}
        {t.start}
      </Button>
    );
  } else if (status.state === "building") {
    body = <Building status={status} t={t} />;
    footer = (
      <Button variant="outline" onClick={remove} disabled={discard.isPending}>
        {t.stop}
      </Button>
    );
  } else if (status.state === "failed") {
    body = (
      <Alert>
        {startError
          ? messageFor(startError, t)
          : status.error?.code === "no_space"
            ? t.noSpace
            : t.failed}
      </Alert>
    );
    footer = (
      <Button onClick={prepare} disabled={start.isPending}>
        {t.tryAgain}
      </Button>
    );
  } else {
    body = <Ready status={status} t={t} locale={locale} />;
    footer = (
      <Button variant="outline" onClick={remove} disabled={discard.isPending}>
        {t.deleteNow}
      </Button>
    );
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-[480px]">
        <DialogHeader>
          <DialogTitle>{t.title}</DialogTitle>
          <DialogDescription>{t.description}</DialogDescription>
        </DialogHeader>
        <p className="text-muted-foreground text-sm">{t.onlyYours}</p>
        {/* Read out as it changes: preparing, then ready. */}
        <div aria-live="polite" className="flex flex-col gap-3">
          {body}
          {deleteFailed && <Alert>{t.deleteFailed}</Alert>}
        </div>
        {footer && <DialogFooter>{footer}</DialogFooter>}
      </DialogContent>
    </Dialog>
  );
}
