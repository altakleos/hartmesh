"use client";

import {
  ActivityIcon,
  CheckCircle2Icon,
  ClipboardIcon,
  DownloadIcon,
  FileCheck2Icon,
  LoaderCircleIcon,
  RefreshCwIcon,
  ShieldAlertIcon,
} from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { toast } from "sonner";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from "@/components/ui/sheet";
import {
  fetchEvidenceBundle,
  type EvidenceSandboxDiagnostic,
  type EvidenceSection,
  type EvidenceState,
  type EvidenceTimelineItem,
  useThreadEvidence,
} from "@/core/evidence";
import { useI18n } from "@/core/i18n/hooks";

const sectionOrder = [
  "admission",
  "assembly",
  "policy",
  "tools",
  "batches",
  "sandbox",
  "retrieval",
  "mcp",
  "artifacts",
] as const;

function short(value: string): string {
  return value.length > 14 ? `${value.slice(0, 10)}…${value.slice(-4)}` : value;
}

export function ThreadEvidence({ threadId }: { threadId: string }) {
  const { t } = useI18n();
  const [open, setOpen] = useState(false);
  const [downloading, setDownloading] = useState(false);
  const bundleController = useRef<AbortController | null>(null);
  const query = useThreadEvidence(threadId, { enabled: open });
  const evidence = query.data;

  useEffect(
    () => () => {
      bundleController.current?.abort();
    },
    [],
  );

  async function copyReference(value: string) {
    await navigator.clipboard.writeText(value);
    toast.success(t.evidence.copied);
  }

  async function downloadBundle() {
    if (!evidence || downloading) return;
    const controller = new AbortController();
    bundleController.current = controller;
    setDownloading(true);
    try {
      const { blob, filename } = await fetchEvidenceBundle(
        threadId,
        evidence.runId,
        controller.signal,
      );
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = filename;
      anchor.click();
      URL.revokeObjectURL(url);
    } catch (error) {
      if (controller.signal.aborted) return;
      toast.error(
        error instanceof Error ? error.message : t.evidence.bundleFailed,
      );
    } finally {
      if (bundleController.current === controller) {
        bundleController.current = null;
      }
      setDownloading(false);
    }
  }

  return (
    <Sheet open={open} onOpenChange={setOpen}>
      <SheetTrigger asChild>
        <Button
          type="button"
          variant="outline"
          size="sm"
          aria-label={t.evidence.label}
          data-testid="evidence-trigger"
        >
          <FileCheck2Icon />
          <span className="hidden xl:inline">{t.evidence.label}</span>
        </Button>
      </SheetTrigger>
      <SheetContent className="w-[min(96vw,620px)] gap-0 p-0 sm:max-w-[620px]">
        <SheetHeader className="border-border border-b px-5 py-4">
          <SheetTitle className="flex items-center gap-2">
            <FileCheck2Icon className="size-4" />
            {t.evidence.title}
          </SheetTitle>
          <SheetDescription>{t.evidence.description}</SheetDescription>
        </SheetHeader>
        <div className="min-h-0 flex-1 overflow-y-auto p-4">
          {query.isLoading ? (
            <div
              role="status"
              className="text-muted-foreground flex justify-center gap-2 py-12 text-sm"
            >
              <LoaderCircleIcon className="size-4 animate-spin" />
              {t.common.loading}
            </div>
          ) : query.isError ? (
            <div
              role="alert"
              className="border-destructive/30 bg-destructive/5 rounded-xl border p-4"
            >
              <p className="text-destructive text-sm font-medium">
                {t.evidence.loadFailed}
              </p>
              <p className="text-muted-foreground mt-1 text-xs">
                {query.error.message}
              </p>
              <Button
                className="mt-3"
                size="sm"
                variant="outline"
                onClick={() => void query.refetch()}
              >
                <RefreshCwIcon /> {t.evidence.retry}
              </Button>
            </div>
          ) : !evidence ? (
            <p className="text-muted-foreground py-12 text-center text-sm">
              {t.evidence.empty}
            </p>
          ) : (
            <EvidenceBody
              evidence={evidence.summary}
              downloading={downloading}
              onCancelDownload={() => bundleController.current?.abort()}
              onCopy={copyReference}
              onDownload={downloadBundle}
            />
          )}
        </div>
      </SheetContent>
    </Sheet>
  );
}

function EvidenceBody({
  evidence,
  downloading,
  onCopy,
  onCancelDownload,
  onDownload,
}: {
  evidence: NonNullable<
    ReturnType<typeof useThreadEvidence>["data"]
  >["summary"];
  downloading: boolean;
  onCopy: (value: string) => Promise<void>;
  onCancelDownload: () => void;
  onDownload: () => Promise<void>;
}) {
  const { t } = useI18n();
  const overview = evidence.overview;
  const artifacts = evidence.sections.artifacts;
  const canDownload = artifacts?.data.bundle_state === "available";
  return (
    <div className="space-y-5" data-testid="evidence-panel">
      <section aria-labelledby="evidence-overview-heading">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <h2 id="evidence-overview-heading" className="text-sm font-semibold">
            {t.evidence.overview}
          </h2>
          <QualificationBadge state={evidence.qualification.state} />
        </div>
        <dl className="border-border bg-card mt-2 grid grid-cols-[auto_1fr] gap-x-4 gap-y-2 rounded-xl border p-4 text-xs">
          <dt className="text-muted-foreground">{t.evidence.status}</dt>
          <dd className="font-medium">{overview.status}</dd>
          <dt className="text-muted-foreground">{t.evidence.accepted}</dt>
          <dd>{overview.accepted_at}</dd>
          <dt className="text-muted-foreground">{t.evidence.terminalReason}</dt>
          <dd>{reasonLabel(overview.terminal_reason, t.evidence.reason)}</dd>
          <dt className="text-muted-foreground">{t.evidence.policy}</dt>
          <dd>
            {overview.policy
              ? `${overview.policy.profile} · ${short(overview.policy.digest)}`
              : t.evidence.state.legacy}
          </dd>
          <dt className="text-muted-foreground">{t.evidence.completeness}</dt>
          <dd>{overview.completeness.replaceAll("_", " ")}</dd>
        </dl>
        <div className="mt-2 flex flex-wrap gap-2">
          <Button
            type="button"
            size="sm"
            variant="outline"
            onClick={() => void onCopy(overview.run_ref)}
          >
            <ClipboardIcon /> {t.evidence.copy} · {short(overview.run_ref)}
          </Button>
          {canDownload && (
            <>
              <Button
                type="button"
                size="sm"
                variant="outline"
                disabled={downloading}
                onClick={() => void onDownload()}
              >
                {downloading ? (
                  <LoaderCircleIcon className="animate-spin" />
                ) : (
                  <DownloadIcon />
                )}
                {downloading
                  ? t.evidence.generatingBundle
                  : t.evidence.downloadBundle}
              </Button>
              {downloading && (
                <Button
                  type="button"
                  size="sm"
                  variant="ghost"
                  onClick={onCancelDownload}
                >
                  {t.evidence.cancelBundle}
                </Button>
              )}
            </>
          )}
        </div>
      </section>

      <section aria-labelledby="evidence-timeline-heading">
        <h2 id="evidence-timeline-heading" className="text-sm font-semibold">
          {t.evidence.timeline}
        </h2>
        {evidence.timeline.length ? (
          <ol className="mt-2 space-y-2">
            {evidence.timeline.map((item) => (
              <TimelineItem key={item.seq} item={item} />
            ))}
          </ol>
        ) : (
          <p className="text-muted-foreground mt-2 text-xs">
            {t.evidence.noDecisions}
          </p>
        )}
      </section>

      <section aria-labelledby="evidence-diagnostics-heading">
        <h2 id="evidence-diagnostics-heading" className="text-sm font-semibold">
          {t.evidence.sandboxDiagnostics}
        </h2>
        {evidence.sandbox_diagnostics?.length ? (
          <ol className="mt-2 space-y-2">
            {evidence.sandbox_diagnostics.map((item) => (
              <DiagnosticItem key={item.seq} item={item} />
            ))}
          </ol>
        ) : (
          <p className="text-muted-foreground mt-2 text-xs">
            {t.evidence.noDiagnostics}
          </p>
        )}
      </section>

      <section aria-labelledby="evidence-sections-heading">
        <h2 id="evidence-sections-heading" className="text-sm font-semibold">
          {t.evidence.sections}
        </h2>
        <div className="mt-2 space-y-2">
          {sectionOrder.map((name) => {
            const section = evidence.sections[name];
            return section ? (
              <EvidenceSectionRow key={name} name={name} section={section} />
            ) : null;
          })}
        </div>
      </section>
    </div>
  );
}

function TimelineItem({ item }: { item: EvidenceTimelineItem }) {
  const { t } = useI18n();
  return (
    <li className="border-border flex gap-3 rounded-lg border p-3 text-xs">
      <ShieldAlertIcon className="mt-0.5 size-4 shrink-0" aria-hidden="true" />
      <div>
        <p className="font-medium">
          {item.decision === "stop" ? t.evidence.stop : t.evidence.warning}:{" "}
          {reasonLabel(item.reason_code, t.evidence.reason)}
        </p>
        <p className="text-muted-foreground mt-1">
          {item.current} / {item.limit}
          {item.at ? ` · ${item.at}` : ""}
        </p>
      </div>
    </li>
  );
}

function DiagnosticItem({ item }: { item: EvidenceSandboxDiagnostic }) {
  const { t } = useI18n();
  const facts = Object.entries(item.facts);
  return (
    <li
      className="border-border flex gap-3 rounded-lg border p-3 text-xs"
      data-testid="evidence-diagnostic"
    >
      <ActivityIcon className="mt-0.5 size-4 shrink-0" aria-hidden="true" />
      <div className="min-w-0">
        <p className="flex flex-wrap items-center gap-2 font-medium">
          <span>{t.evidence.diagnostic[item.kind] ?? item.kind}</span>
          <Badge variant="outline">
            {t.evidence.sessionKind[item.session_kind] ?? item.session_kind}
          </Badge>
        </p>
        {facts.length > 0 && (
          <dl className="mt-1 flex flex-wrap gap-x-3 gap-y-1">
            {facts.map(([key, value]) => (
              <div className="flex gap-1" key={key}>
                <dt className="text-muted-foreground">
                  {key.replaceAll("_", " ")}
                </dt>
                <dd className="break-all">{String(value)}</dd>
              </div>
            ))}
          </dl>
        )}
        <p className="text-muted-foreground mt-1">
          {item.at}
          {item.dropped > 0 ? ` · ${item.dropped} dropped` : ""}
        </p>
      </div>
    </li>
  );
}

function EvidenceSectionRow({
  name,
  section,
}: {
  name: string;
  section: EvidenceSection;
}) {
  const { t } = useI18n();
  return (
    <details className="border-border rounded-lg border p-3">
      <summary className="flex cursor-pointer list-none items-center justify-between gap-3 text-sm font-medium">
        <span>{t.evidence.section[name] ?? name}</span>
        <StateBadge state={section.state} />
      </summary>
      <dl className="mt-3 grid grid-cols-[auto_1fr] gap-x-3 gap-y-2 text-xs">
        {Object.entries(section.data).map(([key, value]) => (
          <div className="contents" key={key}>
            <dt className="text-muted-foreground">
              {key.replaceAll("_", " ")}
            </dt>
            <dd className="min-w-0 break-words">{displayValue(value)}</dd>
          </div>
        ))}
      </dl>
    </details>
  );
}

function StateBadge({ state }: { state: EvidenceState }) {
  const { t } = useI18n();
  return <Badge variant="outline">{t.evidence.state[state] ?? state}</Badge>;
}

function QualificationBadge({ state }: { state: string }) {
  const { t } = useI18n();
  return (
    <Badge
      variant="outline"
      aria-label={`Qualification: ${t.evidence.qualification[state] ?? state}`}
    >
      {state === "qualified" && <CheckCircle2Icon aria-hidden="true" />}
      {t.evidence.qualification[state] ?? state}
    </Badge>
  );
}

function displayValue(value: unknown): string {
  if (value === null || value === undefined) return "—";
  if (
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean"
  )
    return String(value);
  return JSON.stringify(value);
}

function reasonLabel(
  reason: string | null,
  labels: Record<string, string>,
): string {
  if (!reason) return "—";
  return labels[reason] ?? reason.replaceAll("_", " ");
}
