"use client";

import { useEffect, useRef, useState } from "react";
import { toast } from "sonner";

import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSub,
  DropdownMenuSubContent,
  DropdownMenuSubTrigger,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { resolveConversationActions } from "@/core/extensions/actions";
import type {
  ConversationAction,
  ConversationActionContext,
  FrontendContribution,
} from "@/core/extensions/contracts";
import {
  useFrontendServices,
  useFrontendExtensions,
} from "@/core/extensions/hooks";
import { awaitPluginOperation } from "@/core/extensions/operations";
import {
  extensionIcon,
  activeFrontendExtensions,
} from "@/core/extensions/registry";
import { bindFrontendServices } from "@/core/extensions/services";
import { useFileActionLifetime } from "@/core/file-areas/file-action-lifetime";
import { useI18n } from "@/core/i18n/hooks";

import { Tooltip } from "./tooltip";

/** Shared host slot used by the chat toolbar AND every sidebar conversation. */
export function ConversationExtensionActions({
  context,
  placement = "toolbar",
}: {
  context: ConversationActionContext;
  placement?: "toolbar" | "menu";
}) {
  const { t, locale } = useI18n();
  const query = useFrontendExtensions();
  const services = useFrontendServices();
  const lifetime = useFileActionLifetime();
  const controller = useRef(new AbortController());
  const [busy, setBusy] = useState(false);
  const entries = query.isError ? [] : (query.data ?? []);
  useEffect(() => {
    if (controller.current.signal.aborted)
      controller.current = new AbortController();
    const current = controller.current;
    const parentSignal = lifetime.signal;
    const abort = () => current.abort();
    parentSignal.addEventListener("abort", abort, { once: true });
    setBusy(false);
    return () => {
      current.abort();
      parentSignal.removeEventListener("abort", abort);
    };
  }, [context.thread.thread_id, query.data, lifetime]);

  async function execute(
    action: ConversationAction,
    contribution: FrontendContribution,
  ) {
    const signal = controller.current.signal;
    if (!lifetime.active || signal.aborted) return;
    setBusy(true);
    try {
      await awaitPluginOperation(
        action.execute(
          { ...context, signal },
          bindFrontendServices(services, contribution, signal),
        ),
        signal,
      );
    } catch {
      if (lifetime.active && !signal.aborted) {
        lifetime.toasts.add(toast.error(t.extensions.actionFailed));
      }
    } finally {
      if (lifetime.active && !signal.aborted) setBusy(false);
    }
  }

  return activeFrontendExtensions(entries).map(
    ({ contribution, extension }) => {
      if (context.messages?.length === 0) return null;
      const group = resolveConversationActions(
        extension,
        contribution,
        t,
        locale,
      );
      if (!group) return null;
      const actions = group.actions;
      const GroupIcon = extensionIcon(group.icon);
      const items = actions.map((action) => {
        const Icon = extensionIcon(action.icon);
        return (
          <DropdownMenuItem
            key={action.id}
            disabled={busy}
            onSelect={() => void execute(action, contribution)}
          >
            <Icon className="text-muted-foreground" />
            <span>{action.label}</span>
          </DropdownMenuItem>
        );
      });
      if (placement === "menu")
        return (
          <DropdownMenuSub key={contribution.namespace}>
            <DropdownMenuSubTrigger>
              <GroupIcon className="text-muted-foreground" />
              <span>{group.label}</span>
            </DropdownMenuSubTrigger>
            <DropdownMenuSubContent>{items}</DropdownMenuSubContent>
          </DropdownMenuSub>
        );
      return (
        <DropdownMenu key={contribution.namespace}>
          <Tooltip content={group.label}>
            <DropdownMenuTrigger asChild>
              <Button
                aria-label={group.label}
                className="text-muted-foreground hover:text-foreground"
                variant="ghost"
                disabled={busy}
              >
                <GroupIcon />
                <span className="hidden sm:inline">{group.label}</span>
              </Button>
            </DropdownMenuTrigger>
          </Tooltip>
          <DropdownMenuContent align="end">{items}</DropdownMenuContent>
        </DropdownMenu>
      );
    },
  );
}
