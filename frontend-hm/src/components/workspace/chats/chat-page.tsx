"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { toast } from "sonner";

import { type PromptInputMessage } from "@/components/ai-elements/prompt-input";
import { SidebarTrigger } from "@/components/ui/sidebar";
import { ArtifactTrigger } from "@/components/workspace/artifacts";
import { BrowserTrigger } from "@/components/workspace/browser-view";
import { ContextUsageBadge } from "@/components/workspace/context-usage-badge";
import { ConversationExtensionActions } from "@/components/workspace/conversation-extension-actions";
import { ExportTrigger } from "@/components/workspace/export-trigger";
import { GoalStatus } from "@/components/workspace/goal-status";
import {
  InputBox,
  type InputBoxSubmitOptions,
} from "@/components/workspace/input-box";
import { AgentAvailabilityNotice } from "@/components/workspace/instances/agent-header";
import {
  MessageList,
  MESSAGE_LIST_DEFAULT_PADDING_BOTTOM,
} from "@/components/workspace/messages";
import { ThreadContext } from "@/components/workspace/messages/context";
import {
  useSupervisorName,
  usePersonName,
} from "@/components/workspace/person-name";
import {
  SidecarProvider,
  SidecarTrigger,
} from "@/components/workspace/sidecar";
import { ThreadBackgroundTasks } from "@/components/workspace/thread-background-tasks";
import { ThreadScheduledTasksLink } from "@/components/workspace/thread-scheduled-tasks-link";
import { ThreadSubagentBatches } from "@/components/workspace/thread-subagent-batches";
import { ThreadTitle } from "@/components/workspace/thread-title";
import { TodoList } from "@/components/workspace/todo-list";
import { TokenUsageIndicator } from "@/components/workspace/token-usage-indicator";
import { useActiveGoal } from "@/components/workspace/use-active-goal";
import { Welcome } from "@/components/workspace/welcome";
import { useConversationAttachments } from "@/core/agent-instances/conversation-attachments";
import { useConversationDetails } from "@/core/agent-instances/conversation-details";
import {
  conversationIdentity,
  conversationSendBlock,
} from "@/core/agent-instances/conversation-presentation";
import {
  hasFinalAgentAnswer,
  useNewestAgentRun,
} from "@/core/agent-instances/newest-run";
import { agentBlock, INSPECT } from "@/core/agent-instances/presentation";
import { useAuth } from "@/core/auth/AuthProvider";
import {
  useAgentInstances,
  useBrowserControlEnabled,
  useStorage,
  useBranding,
} from "@/core/features";
import { fileAreaNotice } from "@/core/file-areas/storage-state";
import { useI18n } from "@/core/i18n/hooks";
import {
  buildHumanInputResponseText,
  hasOpenHumanInputRequest,
  type HumanInputRequest,
  type HumanInputResponse,
} from "@/core/messages/human-input";
import { isHiddenFromUIMessage } from "@/core/messages/utils";
import { useModels } from "@/core/models/hooks";
import { useNotification } from "@/core/notification/hooks";
import { useLocalSettings, useThreadSettings } from "@/core/settings";
import {
  useBranchThread,
  useThreadMetadata,
  useThreadStream,
  useThreadTokenUsage,
} from "@/core/threads/hooks";
import {
  selectContextUsage,
  threadTokenUsageToTokenUsage,
} from "@/core/threads/token-usage";
import { textOfMessage } from "@/core/threads/utils";
import { env } from "@/env";
import { cn } from "@/lib/utils";

import { AgentWelcome } from "./agent-welcome";
import { ChatBox } from "./chat-box";
import { ConversationAgentHeader } from "./conversation-agent-header";
import { ConversationAttachmentChips } from "./conversation-attachment-chips";
import { ConversationFileDropHint } from "./conversation-file-drop-hint";
import { ConversationFilesContext } from "./conversation-files";
import { FilesStateNotice } from "./files-state-notice";
import { InstanceRunNotice } from "./instance-run-notice";
import { useSpecificChatMode } from "./use-chat-mode";
import { useConversationStop } from "./use-conversation-stop";
import { usePrewarmWorkspace } from "./use-prewarm-workspace";
import { useThreadChat } from "./use-thread-chat";

export default function ChatPage() {
  const { t } = useI18n();
  const { user } = useAuth();
  const locallyCreated = useRef<string | null>(null);
  const feature = useAgentInstances();
  const router = useRouter();
  const { threadId, setThreadId, isNewThread, setIsNewThread, isMock } =
    useThreadChat();
  // A new thread's sandbox starts building the moment the page opens, so the
  // first turn does not pay the cold start while the person watches.
  usePrewarmWorkspace({ threadId, enabled: isNewThread && !isMock });
  // `isNewThread` tracks whether the backend has the thread yet — gates the
  // SDK's history fetch (see issue #2746).  `isWelcomeMode` is the visual
  // welcome layout (centered input, hero, quick actions); we flip it to false
  // the moment the user submits so the UI animates immediately, even though
  // `isNewThread` stays true until the backend actually creates the thread.
  const [isWelcomeMode, setIsWelcomeMode] = useState(isNewThread);
  const [settings, setSettings] = useThreadSettings(threadId);
  const [localSettings, setLocalSettings] = useLocalSettings();
  const { enabled: browserControlEnabled } = useBrowserControlEnabled();
  const storage = useStorage();
  const branding = useBranding();
  const { tokenUsageEnabled } = useModels();
  const threadTokenUsage = useThreadTokenUsage(
    isNewThread || isMock ? undefined : threadId,
    { enabled: !isMock },
  );
  const threadMetadata = useThreadMetadata(threadId, {
    enabled: !isNewThread && !isMock,
    isMock,
  });
  const identity = conversationIdentity({
    threadId,
    isNew: isNewThread,
    isMock,
    createdHere: locallyCreated.current,
    row: threadMetadata.data,
    rowKnown: threadMetadata.isSuccess && !threadMetadata.isError,
  });
  const binding = identity.agent;
  const legacyCapabilities = identity.legacyCapabilities;
  const branchThread = useBranchThread();
  const backendTokenUsage = threadTokenUsageToTokenUsage(threadTokenUsage.data);
  const contextUsage = selectContextUsage(threadTokenUsage.data);
  const mountedRef = useRef(false);
  useSpecificChatMode();

  useEffect(() => {
    mountedRef.current = true;
  }, []);

  // Keep welcome layout in sync when navigating between threads (sidebar
  // clicks, "new chat" button).  Submitting in /chats/new flips the layout
  // via onSend below — `isNewThread` stays true until onStart, so this effect
  // is harmless during the submit transition.
  useEffect(() => {
    setIsWelcomeMode(isNewThread);
  }, [isNewThread]);

  const { showNotification } = useNotification();
  const [localSender, setLocalSender] = useState<string | null>(null);
  const stopCallbacks = useRef<{ clear: () => void } | null>(null);
  const wasStreaming = useRef(false);
  const newestRunCallbacks = useRef<Pick<
    ReturnType<typeof useNewestAgentRun>,
    "sent" | "admitted"
  > | null>(null);

  const {
    thread,
    pendingUsageMessages,
    sendMessage,
    regenerateMessage,
    editAndRegenerateMessage,
    isUploading,
    isHistoryLoading,
    hasMoreHistory,
    loadMoreHistory,
    runRefusal,
    agentRunRefusal,
    refusedNetworkRequests,
  } = useThreadStream({
    threadId: isNewThread ? undefined : threadId,
    displayThreadId: threadId,
    agentInstanceId: binding?.id,
    context: settings.context,
    isMock,
    // onSend only animates the UI; do NOT flip `isNewThread` here — the
    // LangGraph SDK eagerly fetches /history the moment it receives a
    // thread id and assumes the thread exists on the backend (issue #2746).
    onSend: () => {
      newestRunCallbacks.current?.sent();
      stopCallbacks.current?.clear();
      setLocalSender(threadId);
      setIsWelcomeMode(false);
    },
    onAdmitted: (createdThreadId, runId) => {
      newestRunCallbacks.current?.admitted(createdThreadId, runId);
    },
    onStart: (createdThreadId) => {
      if (isNewThread) locallyCreated.current = createdThreadId;
      // ! Important: Never use next.js router for navigation in this case, otherwise it will cause the thread to re-mount and lose all states. Use native history API instead.
      history.replaceState(null, "", `/workspace/chats/${createdThreadId}`);
      setThreadId(createdThreadId);
      setIsNewThread(false);
    },
    onFinish: (state) => {
      if (document.hidden || !document.hasFocus()) {
        let body = "Conversation finished";
        const lastMessage = state.messages.at(-1);
        if (lastMessage) {
          const textContent = textOfMessage(lastMessage);
          if (textContent) {
            body =
              textContent.length > 200
                ? textContent.substring(0, 200) + "..."
                : textContent;
          }
        }
        showNotification(state.title, { body });
      }
    },
  });

  const resident = useConversationDetails(binding?.id ?? null, {
    enabled: feature.known && feature.enabled && !!binding,
    refreshKey: `${thread.isLoading}:${agentRunRefusal?.sequence ?? 0}`,
  });
  const instance = resident.detail;
  const stop = useConversationStop({
    threadId,
    binding,
    detail: instance,
    legacy: legacyCapabilities,
    streaming: thread.isLoading,
    sentHere: localSender === threadId,
    stopStream: () => thread.stop(),
    changed: () => {
      void resident.refresh();
    },
  });
  stopCallbacks.current = stop;
  useEffect(() => {
    if (wasStreaming.current && !thread.isLoading) setLocalSender(null);
    wasStreaming.current = thread.isLoading;
  }, [thread.isLoading]);
  const newestRun = useNewestAgentRun({
    threadId,
    enabled: !!binding && !isMock,
    streaming: thread.isLoading,
    finalAnswer: hasFinalAgentAnswer(thread.messages),
  });
  newestRunCallbacks.current = newestRun;
  const supervisor = useSupervisorName(instance?.supervisor.subject_id ?? "");
  const requesterName = usePersonName(binding?.requester_id ?? "", true);
  const sendBlock = binding
    ? conversationSendBlock({
        binding,
        detail: instance,
        feature,
        detailStatus: resident.status,
        pendingLifecycle: resident.pending,
      })
    : legacyCapabilities
      ? null
      : "loading";
  const agentDisabled = !legacyCapabilities && sendBlock !== null;
  const attachedFiles = useConversationAttachments({
    instanceId: binding?.id ?? null,
    threadId,
    enabled: !!binding && !agentDisabled,
    changed: () => {
      void resident.refresh();
    },
  });
  const fileSentence =
    binding && resident.status === "not-found"
      ? t.agentInstances.accessWithdrawn(binding.name)
      : binding && instance && !(instance.permissions & INSPECT)
        ? t.agentInstances.fileNeedsInspect(binding.name, supervisor)
        : null;
  const fileLocked =
    !legacyCapabilities &&
    (!binding || !instance || !(instance.permissions & INSPECT));
  const availability = instance ? agentBlock(instance, feature) : null;
  const sendNotice =
    binding && sendBlock ? (
      sendBlock === "deleted" ? (
        t.agentInstances.deletedNotice(binding.name)
      ) : sendBlock === "archived" ? (
        t.agentInstances.retiredNotice(binding.name)
      ) : sendBlock === "suspended" ? (
        t.agentInstances.pausedNotice(binding.name)
      ) : sendBlock === "changing" ? (
        t.agentInstances.pending(binding.name)
      ) : sendBlock === "withdrawn" ? (
        t.agentInstances.accessWithdrawn(binding.name)
      ) : sendBlock === "model-off" ? (
        t.sidebar.agentsUnavailable
      ) : sendBlock === "no-use" ? (
        t.agentInstances.conversations.noUse(supervisor)
      ) : sendBlock === "loading" ? null : instance && availability ? (
        <AgentAvailabilityNotice instance={instance} block={availability} />
      ) : (
        t.agentInstances.operationError
      )
    ) : null;
  const hasThreadMessages = thread.messages.length > 0;
  const firstConversationFull =
    runRefusal?.newThread === true &&
    runRefusal.phase === "send" &&
    runRefusal.refusal.code === "files-full";
  const fileNotice =
    legacyCapabilities && !isMock
      ? firstConversationFull
        ? runRefusal.refusal
        : fileAreaNotice(
            storage,
            runRefusal?.phase === "send" &&
              !(
                storage.known &&
                storage.dataUpdatedAt > (runRefusal.storageObservedAt ?? 0)
              )
              ? runRefusal.refusal
              : null,
            "chat",
          )
      : null;
  const filesUnavailable = fileNotice?.code === "storage-unavailable";

  useEffect(() => {
    if (
      !isNewThread &&
      !isMock &&
      threadMetadata.data === null &&
      !threadMetadata.isLoading &&
      !threadMetadata.isFetching &&
      !isHistoryLoading &&
      !hasMoreHistory &&
      !hasThreadMessages
    ) {
      router.replace("/workspace/chats/new");
    }
  }, [
    hasMoreHistory,
    hasThreadMessages,
    isHistoryLoading,
    isMock,
    isNewThread,
    router,
    threadMetadata.data,
    threadMetadata.isFetching,
    threadMetadata.isLoading,
  ]);

  const handleSubmit = useCallback(
    (message: PromptInputMessage, options?: InputBoxSubmitOptions) => {
      if (binding) {
        if (agentDisabled || attachedFiles.uploading)
          return Promise.reject(
            new Error("Conversation is not ready to send."),
          );
        const capture = attachedFiles.capture();
        return sendMessage(threadId, { ...message, files: [] }, undefined, {
          ...options,
          additionalKwargs: {
            ...options?.additionalKwargs,
            files: capture.files,
          },
          onSent: () => {
            capture.admitted();
            options?.onSent?.();
          },
        });
      }
      if (!legacyCapabilities)
        return Promise.reject(
          new Error("Conversation binding is not available."),
        );
      // PromptInput preserves both text and files for a rejected admission,
      // and clears only the captured submission after the server admits it.
      return sendMessage(threadId, message, undefined, options);
    },
    [
      sendMessage,
      threadId,
      binding,
      agentDisabled,
      attachedFiles,
      legacyCapabilities,
    ],
  );
  const handleSubmitHumanInput = useCallback(
    async (request: HumanInputRequest, response: HumanInputResponse) => {
      let sent = false;
      await sendMessage(
        threadId,
        {
          text: buildHumanInputResponseText(request, response),
          files: [],
        },
        undefined,
        {
          additionalKwargs: {
            hide_from_ui: true,
            human_input_response: response,
          },
          onSent: () => {
            sent = true;
          },
        },
      );
      return sent;
    },
    [sendMessage, threadId],
  );
  const handleRegenerate = useCallback(
    (messageId: string, supersededMessageIds: string[]) =>
      regenerateMessage(threadId, messageId, supersededMessageIds),
    [regenerateMessage, threadId],
  );
  const handleEditAndRegenerate = useCallback(
    (messageId: string, replacementText: string) =>
      editAndRegenerateMessage(threadId, messageId, replacementText),
    [editAndRegenerateMessage, threadId],
  );
  const handleBranchTurn = useCallback(
    async (messageId: string, messageIds: string[]) => {
      if (
        isNewThread ||
        isMock ||
        env.NEXT_PUBLIC_STATIC_WEBSITE_ONLY === "true"
      ) {
        return;
      }

      try {
        const response = await branchThread.mutateAsync({
          threadId,
          messageId,
          messageIds,
        });
        toast.success(t.conversation.branchCreated);
        router.push(`/workspace/chats/${response.thread_id}`);
      } catch (error) {
        toast.error(
          error instanceof Error ? error.message : t.conversation.branchFailed,
        );
      }
    },
    [branchThread, isMock, isNewThread, router, t, threadId],
  );

  const tokenUsageInlineMode = tokenUsageEnabled
    ? localSettings.tokenUsage.inlineMode
    : "off";
  const hasTodos = (thread.values.todos?.length ?? 0) > 0;
  const browserEnabled =
    !isNewThread && !isMock && browserControlEnabled && legacyCapabilities;
  const { activeGoal, hasGoal, setLocalGoal } = useActiveGoal(
    threadId,
    thread.values.goal,
  );
  const hasOpenHumanInputCard = useMemo(
    () =>
      hasOpenHumanInputRequest(
        thread.messages,
        (message) => !isHiddenFromUIMessage(message),
      ),
    [thread.messages],
  );

  return (
    <ConversationFilesContext.Provider
      value={{
        locked: fileLocked,
        sentence: fileSentence,
        agent: binding,
        detail: instance,
        providerName: branding.providerName,
        supportURL: branding.supportURL,
        refusedNetworkRequests,
      }}
    >
      <ThreadContext.Provider value={{ thread, isMock }}>
        <SidecarProvider
          parentThreadId={threadId}
          context={settings.context}
          isMock={isMock}
          enabled={legacyCapabilities}
        >
          <ChatBox threadId={threadId} browserEnabled={browserEnabled}>
            <div className="relative flex size-full min-h-0 justify-between">
              {binding && (
                <ConversationFileDropHint
                  key={`${threadId}:${binding.id}`}
                  name={binding.name}
                  enabled={!agentDisabled && !isMock}
                />
              )}
              <header
                className={cn(
                  "absolute top-0 right-0 left-0 flex shrink-0 items-center gap-2 px-2 sm:px-4",
                  binding ? "h-16" : "h-12",
                  isWelcomeMode
                    ? "bg-background/0 z-40 backdrop-blur-none"
                    : "bg-background/80 z-30 shadow-xs backdrop-blur",
                )}
              >
                {!isMock && <SidebarTrigger className="md:hidden" />}
                <div className="flex min-w-0 flex-1 items-center text-sm font-medium">
                  {binding ? (
                    <ConversationAgentHeader
                      binding={binding}
                      detail={instance}
                      alsoWorking={
                        !!instance &&
                        instance.activity.running_runs >
                          Math.max(
                            instance.activity.running.filter(
                              (run) => run.thread_id === threadId,
                            ).length,
                            thread.isLoading ? 1 : 0,
                          )
                      }
                      title={
                        <ThreadTitle
                          threadId={threadId}
                          thread={thread}
                          canonicalTitle={threadMetadata.data?.values?.title}
                        />
                      }
                    />
                  ) : (
                    <ThreadTitle
                      threadId={threadId}
                      thread={thread}
                      canonicalTitle={threadMetadata.data?.values?.title}
                    />
                  )}
                </div>
                <div className="flex shrink-0 items-center gap-2">
                  {!isNewThread &&
                    !isMock &&
                    env.NEXT_PUBLIC_STATIC_WEBSITE_ONLY !== "true" && (
                      <ThreadBackgroundTasks threadId={threadId} />
                    )}
                  {!isNewThread &&
                    !isMock &&
                    env.NEXT_PUBLIC_STATIC_WEBSITE_ONLY !== "true" && (
                      <ThreadSubagentBatches threadId={threadId} />
                    )}
                  {!isNewThread && !isMock && legacyCapabilities && (
                    <ThreadScheduledTasksLink threadId={threadId} />
                  )}
                  {tokenUsageEnabled ? (
                    <TokenUsageIndicator
                      threadId={isNewThread ? undefined : threadId}
                      backendUsage={backendTokenUsage}
                      contextUsage={contextUsage}
                      enabled={tokenUsageEnabled}
                      messages={thread.messages}
                      pendingMessages={pendingUsageMessages}
                      preferences={localSettings.tokenUsage}
                      onPreferencesChange={(preferences) =>
                        setLocalSettings("tokenUsage", preferences)
                      }
                    />
                  ) : (
                    <ContextUsageBadge contextUsage={contextUsage} />
                  )}
                  {legacyCapabilities && <SidecarTrigger />}
                  {binding &&
                    instance &&
                    (instance.permissions & INSPECT) !== 0 && (
                      <Link
                        className="text-sm underline"
                        href={`/workspace/instances?instance=${binding.id}&tab=files`}
                      >
                        {t.agentInstances.home}
                      </Link>
                    )}
                  {browserEnabled && <BrowserTrigger />}
                  <ExportTrigger threadId={threadId} />
                  {!isMock && threadMetadata.data && (
                    <ConversationExtensionActions
                      context={{
                        thread: threadMetadata.data,
                        messages: thread.messages,
                      }}
                    />
                  )}
                  {legacyCapabilities && <ArtifactTrigger />}
                </div>
              </header>
              <main className="flex min-h-0 max-w-full grow flex-col">
                <div className="flex min-h-0 flex-1 justify-center">
                  {binding && !hasThreadMessages && !isHistoryLoading ? (
                    <AgentWelcome
                      key={`${user?.id}:${threadId}:${binding.id}`}
                      binding={binding}
                      detail={instance}
                      threadId={threadId}
                    />
                  ) : (
                    <MessageList
                      archiveDownloadsEnabled={
                        legacyCapabilities &&
                        (isNewThread || isMock || threadMetadata.data != null)
                      }
                      className={cn(
                        "size-full",
                        !isWelcomeMode && (binding ? "pt-16" : "pt-10"),
                      )}
                      testId="main-message-list"
                      threadId={threadId}
                      thread={thread}
                      enableConversationOutline
                      paddingBottom={MESSAGE_LIST_DEFAULT_PADDING_BOTTOM}
                      hasMoreHistory={hasMoreHistory}
                      loadMoreHistory={loadMoreHistory}
                      isHistoryLoading={isHistoryLoading}
                      tokenUsageInlineMode={tokenUsageInlineMode}
                      canRegenerate={
                        !isNewThread &&
                        !isMock &&
                        env.NEXT_PUBLIC_STATIC_WEBSITE_ONLY !== "true" &&
                        !isUploading &&
                        !thread.isLoading
                      }
                      onRegenerateMessage={handleRegenerate}
                      canEdit={
                        !isNewThread &&
                        !isMock &&
                        env.NEXT_PUBLIC_STATIC_WEBSITE_ONLY !== "true" &&
                        !isUploading &&
                        !thread.isLoading &&
                        !branchThread.isPending &&
                        !hasGoal &&
                        !hasOpenHumanInputCard
                      }
                      onEditAndRegenerateMessage={handleEditAndRegenerate}
                      onSubmitHumanInput={
                        isMock ||
                        agentDisabled ||
                        env.NEXT_PUBLIC_STATIC_WEBSITE_ONLY === "true"
                          ? undefined
                          : handleSubmitHumanInput
                      }
                      canBranch={
                        !isNewThread &&
                        !isMock &&
                        env.NEXT_PUBLIC_STATIC_WEBSITE_ONLY !== "true" &&
                        !isUploading &&
                        !thread.isLoading &&
                        !branchThread.isPending
                      }
                      onBranchTurn={handleBranchTurn}
                    />
                  )}
                </div>
                <div
                  className={cn(
                    "right-0 bottom-0 left-0 z-30 flex justify-center px-3 sm:px-4",
                    isWelcomeMode ? "absolute" : "relative shrink-0 pb-4",
                  )}
                >
                  <div
                    className={cn(
                      "relative w-full",
                      isWelcomeMode &&
                        "-translate-y-[calc(50vh-48px)] sm:-translate-y-[calc(50vh-96px)]",
                      isWelcomeMode
                        ? "max-w-(--container-width-sm)"
                        : "max-w-(--container-width-md)",
                    )}
                  >
                    {(hasGoal || hasTodos) && (
                      <div
                        className={cn(
                          "right-0 left-0 z-0",
                          isWelcomeMode ? "absolute -top-4" : "relative",
                        )}
                      >
                        <div
                          className={cn(
                            "right-0 bottom-0 left-0 flex flex-col",
                            isWelcomeMode ? "absolute" : "relative",
                          )}
                        >
                          {activeGoal && <GoalStatus goal={activeGoal} />}
                          {hasTodos && (
                            <TodoList
                              className="bg-background/5"
                              todos={thread.values.todos ?? []}
                              hidden={false}
                            />
                          )}
                        </div>
                      </div>
                    )}
                    {mountedRef.current ? (
                      <>
                        {sendNotice && <div role="alert">{sendNotice}</div>}
                        {stop.message && <div role="alert">{stop.message}</div>}
                        {binding && agentRunRefusal && (
                          <InstanceRunNotice
                            binding={binding}
                            detail={instance}
                            refusal={agentRunRefusal.refusal}
                          />
                        )}
                        {binding &&
                          !agentRunRefusal &&
                          newestRun.notice &&
                          (newestRun.notice.kind === "service" ? (
                            <p role="alert">
                              {t.agentInstances.interruptedByService}
                            </p>
                          ) : (
                            <InstanceRunNotice
                              binding={binding}
                              detail={instance}
                              refusal={newestRun.notice.refusal}
                            />
                          ))}
                        {binding && binding.requester_id !== user?.id && (
                          <p>
                            {t.agentInstances.chat.colleagues(
                              requesterName ?? "…",
                              binding.name,
                            )}
                          </p>
                        )}
                        {fileNotice && (
                          <FilesStateNotice refusal={fileNotice} area="chat" />
                        )}
                        <InputBox
                          allowStop={stop.allowed}
                          stopDisabled={stop.busy || stop.pending}
                          allowAttachments={legacyCapabilities || !!binding}
                          attachmentsUnavailable={filesUnavailable}
                          storageRefusal={runRefusal}
                          agentRefusal={agentRunRefusal}
                          agentFiles={
                            binding
                              ? {
                                  name: binding.name,
                                  uploading: attachedFiles.uploading,
                                  restoredCount: attachedFiles.restored.length,
                                  restoreFiles: attachedFiles.restoreFiles,
                                  content: (
                                    <ConversationAttachmentChips
                                      binding={binding}
                                      detail={instance}
                                      files={attachedFiles}
                                      disabled={agentDisabled}
                                    />
                                  ),
                                }
                              : undefined
                          }
                          className={cn(
                            "bg-background/5 w-full",
                            isWelcomeMode && "-translate-y-2 sm:-translate-y-4",
                          )}
                          isWelcomeMode={isWelcomeMode}
                          threadId={threadId}
                          draftThreadId={isNewThread ? "new" : threadId}
                          autoFocus={isWelcomeMode}
                          status={
                            thread.isLoading
                              ? "streaming"
                              : thread.error
                                ? "error"
                                : "ready"
                          }
                          context={settings.context}
                          extraHeader={
                            isWelcomeMode &&
                            !hasGoal &&
                            !hasTodos && (
                              <Welcome mode={settings.context.mode} />
                            )
                          }
                          disabled={
                            isMock ||
                            env.NEXT_PUBLIC_STATIC_WEBSITE_ONLY === "true" ||
                            isUploading ||
                            firstConversationFull ||
                            agentDisabled ||
                            (stop.busy && !thread.isLoading) ||
                            (!isNewThread && isHistoryLoading)
                          }
                          onContextChange={(context) =>
                            setSettings("context", context)
                          }
                          onGoalChange={setLocalGoal}
                          onSubmit={handleSubmit}
                          onStop={stop.stop}
                        />
                      </>
                    ) : (
                      <div
                        aria-hidden="true"
                        className={cn(
                          "bg-background/5 h-32 w-full rounded-2xl",
                          isWelcomeMode && "-translate-y-2 sm:-translate-y-4",
                        )}
                      />
                    )}
                    {env.NEXT_PUBLIC_STATIC_WEBSITE_ONLY === "true" && (
                      <div className="text-muted-foreground/67 w-full translate-y-12 text-center text-xs">
                        {t.common.notAvailableInDemoMode}
                      </div>
                    )}
                  </div>
                </div>
              </main>
            </div>
          </ChatBox>
        </SidecarProvider>
      </ThreadContext.Provider>
    </ConversationFilesContext.Provider>
  );
}
