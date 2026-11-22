import { useI18n } from "@/core/i18n/hooks";
import { SnapshotError } from "@/core/shared-conversations/api";

export function useSnapshotCopy() {
  const { locale, t } = useI18n();
  const zh = locale === "zh-CN";
  const product = t.pages.appName;
  return {
    locale,
    product,
    group: zh ? "对话" : "Conversations",
    share: zh ? "与所有人分享" : "Share with everyone",
    shareConversation: zh
      ? "与所有人分享此对话"
      : "Share conversation with everyone",
    more: zh ? "加载更多对话" : "Load more conversations",
    confirm: zh ? "分享" : "Share",
    cancel: t.common.cancel,
    retry: t.files.retry,
    loading: t.common.loading,
    download: t.files.download,
    audience: (company: string | null) =>
      zh
        ? `${company ?? "公司"}的所有人及其助手都可以阅读此副本及其文件。`
        : `Everyone at ${company ?? "the company"} and their assistants can read this copy and its files.`,
    needInspect: (supervisor: string) =>
      zh
        ? `分享还需要“可查看所有工作”权限。请联系${supervisor}。`
        : `Sharing also requires “Can see all work”. Ask ${supervisor}.`,
    needManager: (name: string, supervisor: string) =>
      zh
        ? `只有能管理${name}的人可以分享此对话。请联系${supervisor}。`
        : `Only someone who can manage ${name} can share this conversation. Ask ${supervisor}.`,
    after: (company: string | null) =>
      zh
        ? `分享后，${company ?? "公司"}中可读取公司共享区的所有人及其助手都能阅读这些消息并打开文件。`
        : `After sharing, everyone with access to Company shared at ${company ?? "the company"}, and their assistants, can read these messages and open these files.`,
    currentAudience: (
      name: string,
      starter: string | null,
      starterCanRead: boolean,
      inspect: number,
      chat: number,
    ) => {
      const reader = starter
        ? zh
          ? `发起人（${starter}）`
          : `the conversation starter (${starter})`
        : zh
          ? "发起人"
          : "the conversation starter";
      return zh
        ? `目前，可查看${name}所有工作的成员（${inspect} 人）能阅读消息并打开文件。${starterCanRead ? reader + "也能阅读这些消息。" : ""}可与${name}对话的成员（${chat} 人）可以向其询问文件内容。`
        : `Today people who can see all of ${name}’s work (${inspect}) can read the messages and open the files.${starterCanRead ? " " + reader.charAt(0).toUpperCase() + reader.slice(1) + " can read the messages." : ""} People who can chat with ${name} (${chat}) can ask about the files’ contents.`;
    },
    authors: (names: string[]) =>
      zh
        ? `消息作者：${names.join("、")}`
        : `Messages written by ${names.join(", ")}.`,
    currentFiles: (name: string) =>
      zh
        ? `以下文件按${name}的文件区中当前版本分享。未作为结果提供的附件仍留在该文件区。`
        : `The listed files are shared as they are now in ${name}’s files. Attachments that were never presented remain in that file area.`,
    acknowledge: (company: string | null) =>
      zh
        ? `我确认${company ?? "公司"}中可读取公司共享区的所有人都能阅读这些消息和文件。`
        : `I understand that everyone with access to Company shared at ${company ?? "the company"} will be able to read these messages and files.`,
    freeze: zh
      ? "仅分享当前可见的消息和对话中提供的文件。后续消息、工具步骤以及未作为结果提供的附件不会被包含。"
      : "This copy includes the visible messages and the files presented in the conversation. Later messages, tool steps and attachments that were never presented stay out.",
    count: (n: number) =>
      zh ? `${n} 条消息` : `${n} ${n === 1 ? "message" : "messages"}`,
    shared: (company: string | null) =>
      zh
        ? `已与${company ?? "公司"}的所有人分享至「对话」。`
        : `Shared with everyone at ${company ?? "the company"}, in Conversations.`,
    already: zh ? "此版本已分享。" : "This version is already shared.",
    open: zh ? "打开" : "Open",
    undo: t.shared.undo,
    unknown: zh ? "已离开的成员" : "a former member",
    untitled: t.pages.untitled,
    partial: zh
      ? "仅显示部分已分享对话。"
      : "Only part of the shared conversation list is shown.",
    readError: zh
      ? "无法读取已分享的对话。"
      : "Shared conversations could not be read.",
    unavailable: zh
      ? "此对话不可用或未分享。"
      : "This conversation is unavailable or is no longer shared.",
    missing: zh ? "不再分享" : "No longer shared",
    files: zh ? "文件" : "Files",
    transcript: zh ? "文字记录" : "Transcript",
    user: zh ? "用户" : "User",
    by: (who: string, date: string) =>
      zh ? `${who}于${date}分享` : `Shared by ${who} on ${date}`,
    with: (name: string) =>
      zh ? `与${name}的对话` : `Conversation with ${name}`,
    continue: zh ? `使用${product}继续` : `Continue with ${product}`,
    fallback: (who: string) =>
      zh
        ? `接下来将由${product}而非${who}回答。`
        : `${product} will answer, not ${who}.`,
    remove: t.shared.remove,
    removeTitle: zh
      ? "从共享中移除此对话？"
      : "Remove this conversation from Shared?",
    retained: zh ? "保留的文件" : "Files kept",
    removed: zh ? "已从共享中移除。" : "Removed from Shared.",
    copies: zh
      ? "已继续此对话的人将保留其对话和文件副本。"
      : "People who continued from it keep their conversations and their copies of the files.",
    agentCopies: (name: string) =>
      zh
        ? `已存入${name}文件中的副本也将保留。`
        : `Any copies already in ${name}’s files stay there.`,
    copyReason: (reason: string) => {
      const reasons: Record<string, [string, string]> = {
        no_longer_shared: ["no longer shared", "不再共享"],
        access_denied: ["access changed", "访问权限已更改"],
        changed: ["the source changed", "源文件已更改"],
        storage_full: ["storage is full", "存储空间已满"],
        too_large: ["file is too large", "文件过大"],
      };
      return (
        reasons[reason]?.[zh ? 1 : 0] ??
        (zh ? "文件暂不可用" : "file is unavailable")
      );
    },
    alias: (names: string[]) =>
      zh ? `也叫作 ${names.join("、")}` : `Also named ${names.join(", ")}`,
    failedFiles: zh ? "未复制" : "Not copied",
    retryFiles: zh ? "重试复制文件" : "Retry copying files",
    copied: (title: string, who: string, date: string, source?: string) =>
      zh
        ? `复制自${who}于${date}分享的${source ? "与" + source + "的" : ""}对话《${title}》。`
        : source
          ? `Copied from “${title}”, a conversation with ${source} that ${who} shared on ${date}.`
          : `Copied from “${title}”, a conversation ${who} shared on ${date}.`,
    retired: (who: string, date: string) =>
      zh
        ? `复制自${who}于${date}分享的对话，该对话已从共享中移除。`
        : `Copied from a conversation ${who} shared on ${date}, since removed from Shared.`,
    answers: (name: string, other?: string) =>
      zh
        ? `接下来由${name}${other ? "而非" + other : ""}回答。`
        : `${name} answers from here${other ? ", not " + other : ""}.`,
    error: (error: unknown) => {
      if (error instanceof SnapshotError) {
        if (typeof error.detail === "string" && error.detail !== "Not Found")
          return error.detail;
        if (
          error.detail &&
          typeof error.detail === "object" &&
          "message" in error.detail &&
          typeof error.detail.message === "string"
        )
          return error.detail.message;
        const detail = error.detail;
        if (detail && typeof detail === "object" && "code" in detail) {
          switch (detail.code) {
            case "finish-current-turn":
              return zh
                ? "请等待当前轮次结束后再分享。"
                : "Finish the current turn before sharing.";
            case "snapshot-acknowledgement-required":
              return zh
                ? "请确认谁将能够阅读这些消息和文件。"
                : "Confirm who will be able to read these messages and files.";
            case "snapshot-changed":
              return zh
                ? "对话已更改，请重新确认。"
                : "The conversation changed. Review it again.";
            case "conversation-empty":
              return zh
                ? "此对话尚无可分享的消息。"
                : "This conversation has no messages to share.";
            case "snapshot-file-count":
            case "snapshot-path-limit":
            case "snapshot-limit":
              return zh
                ? "此对话超出单次分享的限制。"
                : "This conversation exceeds the limit for one shared copy.";
            case "snapshot-file-changed":
              return zh
                ? "分享期间文件已更改，请重试。"
                : "A file changed while sharing. Try again.";
            case "shared-busy":
              return zh
                ? "共享空间正忙，请稍后重试。"
                : "Shared is busy. Try again.";
            case "snapshot-file-conflict":
              if ("publisher" in detail && typeof detail.publisher === "string")
                return zh
                  ? `${detail.publisher}已在共享位置放置其他文件，请联系该成员或管理员。`
                  : `${detail.publisher} already put a different file there. Ask them or an administrator to remove it.`;
              return zh
                ? "该共享位置已有文件。请联系管理员。"
                : "There is another file at that place in Shared. Ask an administrator to remove it.";
            case "unrecorded-file":
              return zh
                ? "共享位置已有未记录发布者的文件。请联系管理员。"
                : "A file nobody published is at that place in Shared. Ask an administrator to remove it.";
          }
        }
        if (error.status === 404 || error.status === 410)
          return zh
            ? "此对话不可用或不再分享。"
            : "This conversation is unavailable or is no longer shared.";
        if (error.status === 409)
          return zh
            ? "对话或共享状态已更改，请刷新后重试。"
            : "The conversation or its sharing state changed. Refresh and try again.";
      }
      return zh
        ? "无法完成此操作。请重试。"
        : "This operation could not be completed. Try again.";
    },
  };
}
