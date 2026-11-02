"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

import { Button } from "@/components/ui/button";
import { hasCeiling } from "@/core/agent-instances/presentation";
import { useAuth } from "@/core/auth/AuthProvider";
import { useI18n } from "@/core/i18n/hooks";
import {
  type AgentTarget,
  type WebPageKey,
  webPageHref,
} from "@/core/telegram-bots/web-pages";

/**
 * What can reach this list. The server refuses the other four outside an AI
 * assistant's or AI employee's conversation, and those have no such tool yet.
 */
type OfferedWebPageKey = Exclude<
  WebPageKey,
  "access" | "agentSettings" | "files" | "work"
>;

/** The pages a turn offered, once the turn has ended, so a link never moves. */
export function WebPageLinks({
  keys,
  target,
}: {
  keys: WebPageKey[];
  target: AgentTarget;
}) {
  const { t } = useI18n();
  const pathname = usePathname();
  const { user } = useAuth();
  const labels = t.telegramBots.webPage;
  const label: Record<OfferedWebPageKey, string> = {
    memory: labels.memory,
    conversations: labels.conversations,
    addAssistant: labels.addAssistant,
    addEmployee: labels.addEmployee,
    telegram: labels.telegram,
    timezone: labels.telegram,
    scheduled: labels.scheduled,
    account: labels.account,
  };
  const offered = (key: WebPageKey): key is OfferedWebPageKey => key in label;

  const links = keys.filter(offered).flatMap((key) => {
    const hidden =
      (key === "addEmployee" && user?.system_role !== "admin") ||
      (key === "memory" &&
        target.kind === "agent" &&
        !hasCeiling(user?.permissions, "memory:read"));
    const href = hidden ? null : webPageHref(key, target, pathname);
    return href ? [{ key, href, text: label[key] }] : [];
  });

  if (links.length === 0) {
    return null;
  }

  return (
    <div className="flex flex-wrap gap-2" data-testid="web-page-links">
      {links.map(({ key, href, text }) => (
        <Button key={key} asChild size="sm" variant="outline">
          <Link href={href}>{text}</Link>
        </Button>
      ))}
    </div>
  );
}
