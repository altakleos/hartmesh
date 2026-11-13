"use client";

import { ClipboardListIcon } from "lucide-react";
import Link from "next/link";
import { useId } from "react";

import { useI18n } from "@/core/i18n/hooks";
import type { AssignmentDraft } from "@/core/messages/chat-tools";
import { type AgentTarget, webPageHref } from "@/core/telegram-bots/web-pages";

/** Telegram owns confirmation; this historical view only displays the proposal. */
export function AssignmentCard({
  draft,
  target,
}: {
  draft: AssignmentDraft;
  target: AgentTarget;
}) {
  const { t, locale } = useI18n();
  const titleId = useId();
  if (target.kind !== "agent") return null;
  const due =
    draft.due_text ??
    (draft.due_at ? new Date(draft.due_at).toLocaleString(locale) : null);
  const href = webPageHref("work", target, "");
  return (
    <section
      aria-labelledby={titleId}
      className="border-border bg-card/70 text-card-foreground rounded-lg border p-4 shadow-xs"
      data-testid="assignment-card"
    >
      <div className="flex items-start gap-3">
        <div className="bg-primary/10 text-primary mt-0.5 flex size-8 shrink-0 items-center justify-center rounded-md">
          <ClipboardListIcon className="size-4" />
        </div>
        <div className="min-w-0 flex-1 space-y-3">
          <h2 id={titleId} className="text-sm leading-5 font-medium">
            {t.assignmentCard.title(target.name)}
          </h2>
          <dl className="space-y-3 text-sm">
            <div>
              <dt className="text-muted-foreground">{t.agentWork.objective}</dt>
              <dd className="wrap-anywhere whitespace-pre-wrap">
                {draft.objective}
              </dd>
            </div>
            <div>
              <dt className="text-muted-foreground">{t.agentWork.criteria}</dt>
              <dd className="wrap-anywhere whitespace-pre-wrap">
                {draft.success_criteria}
              </dd>
            </div>
            {due && (
              <div>
                <dt className="text-muted-foreground">
                  {t.assignmentCard.deadline}
                </dt>
                <dd className="wrap-anywhere whitespace-pre-wrap">{due}</dd>
              </div>
            )}
          </dl>
          <p className="text-muted-foreground text-sm">
            {t.assignmentCard.note(target.name)}
          </p>
          {href && (
            <Link
              className="text-primary text-sm underline underline-offset-4"
              href={href}
            >
              {t.telegramBots.webPage.work}
            </Link>
          )}
        </div>
      </div>
    </section>
  );
}
