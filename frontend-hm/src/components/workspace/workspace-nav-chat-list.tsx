"use client";

import {
  BotIcon,
  CalendarClock,
  FolderIcon,
  MessagesSquare,
  TriangleAlertIcon,
  type LucideIcon,
} from "lucide-react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { useId } from "react";

import {
  SidebarGroup,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
} from "@/components/ui/sidebar";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { AttentionNav } from "@/components/workspace/attention/nav";
import { useAgentInstances, useSpacesPageVisible } from "@/core/features";
import { useI18n } from "@/core/i18n/hooks";

function Entry({
  href,
  label,
  icon: Icon,
  inert = false,
  explanation,
  warning = false,
}: {
  href: string;
  label: string;
  icon: LucideIcon;
  inert?: boolean;
  explanation?: string;
  warning?: boolean;
}) {
  const pathname = usePathname();
  const id = useId();
  const content = (
    <>
      <Icon />
      <span>{label}</span>
      {warning && (
        <TriangleAlertIcon
          className="ml-auto size-4 text-amber-600"
          aria-hidden
        />
      )}
    </>
  );
  const button = inert ? (
    <SidebarMenuButton
      className="text-muted-foreground/50"
      aria-disabled
      aria-describedby={explanation ? id : undefined}
    >
      {content}
    </SidebarMenuButton>
  ) : (
    <SidebarMenuButton isActive={pathname.startsWith(href)} asChild>
      <Link
        className="text-muted-foreground"
        href={href}
        aria-describedby={explanation ? id : undefined}
      >
        {content}
      </Link>
    </SidebarMenuButton>
  );
  return (
    <SidebarMenuItem>
      {explanation ? (
        <Tooltip>
          <TooltipTrigger asChild>
            <span
              className={
                inert ? "block w-full cursor-not-allowed" : "block w-full"
              }
            >
              {button}
              <span id={id} className="sr-only">
                {explanation}
              </span>
            </span>
          </TooltipTrigger>
          <TooltipContent side="right">{explanation}</TooltipContent>
        </Tooltip>
      ) : (
        button
      )}
    </SidebarMenuItem>
  );
}

export function WorkspaceNavChatList() {
  const { t } = useI18n();
  const agents = useAgentInstances();
  const spacesVisible = useSpacesPageVisible();
  const available = agents.known && agents.enabled && !agents.isLoading;
  const waiting = available && agents.execution === "unavailable";
  function kindState(off: boolean, offText: string) {
    const notOffered =
      available &&
      ((agents.execution === "ready" && off) ||
        agents.reason === "agents-none");
    return {
      inert: !available || notOffered,
      warning: waiting && !notOffered,
      explanation: agents.isLoading
        ? undefined
        : notOffered
          ? offText
          : !available || waiting
            ? t.sidebar.agentsUnavailable
            : undefined,
    };
  }
  return (
    <SidebarGroup className="pt-1">
      <SidebarMenu>
        {available && <AttentionNav />}
        <Entry
          href="/workspace/chats"
          label={t.sidebar.chats}
          icon={MessagesSquare}
        />
        <Entry
          href="/workspace/assistants"
          label={t.sidebar.myAssistants}
          icon={BotIcon}
          {...kindState(
            agents.assistants.perUser === 0,
            t.sidebar.assistantsOff,
          )}
        />
        <Entry
          href="/workspace/employees"
          label={t.sidebar.aiEmployees}
          icon={BotIcon}
          {...kindState(
            agents.employees.allowed === 0,
            t.sidebar.aiEmployeesOff,
          )}
        />
        {spacesVisible && (
          <Entry
            href="/workspace/spaces"
            label={t.storageSpaces.title}
            icon={FolderIcon}
          />
        )}
        <Entry
          href="/workspace/files"
          label={t.sidebar.files}
          icon={FolderIcon}
        />
        <Entry
          href="/workspace/agents"
          label={t.sidebar.agents}
          icon={BotIcon}
          inert={!available}
          explanation={available ? undefined : t.sidebar.agentsDisabledTooltip}
        />
        <Entry
          href="/workspace/scheduled-tasks"
          label={t.sidebar.scheduledTasks}
          icon={CalendarClock}
        />
      </SidebarMenu>
    </SidebarGroup>
  );
}
