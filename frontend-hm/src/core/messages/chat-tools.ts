import type { Message } from "@langchain/langgraph-sdk";
import { z } from "zod";

import { isWebPageKey, type WebPageKey } from "@/core/telegram-bots/web-pages";

import type { MessageGroup } from "./utils";

const LINKS_MAX = 3;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

/** The page a `show_web_page` tool message offers, or null for anything else. */
export function extractWebPageKey(message: Message): WebPageKey | null {
  if (message.type !== "tool") {
    return null;
  }
  const artifact: unknown = Reflect.get(message, "artifact");
  const page = isRecord(artifact) ? artifact.web_page : null;
  return isRecord(page) &&
    page.version === 1 &&
    page.kind === "web_page" &&
    isWebPageKey(page.key)
    ? page.key
    : null;
}

/** A turn's page keys belong to its last group, in order, without repeats, at most three. */
export function getWebPageLinksByGroupIndex(
  groups: MessageGroup[],
): Map<number, WebPageKey[]> {
  const links = new Map<number, WebPageKey[]>();
  let keys: WebPageKey[] = [];
  let lastGroup = -1;
  const endTurn = () => {
    if (keys.length > 0) {
      links.set(lastGroup, keys);
    }
    keys = [];
  };

  groups.forEach((group, index) => {
    if (group.type === "human") {
      endTurn();
      return;
    }
    lastGroup = index;
    for (const message of group.messages) {
      const key = extractWebPageKey(message);
      if (key && !keys.includes(key) && keys.length < LINKS_MAX) {
        keys = [...keys, key];
      }
    }
  });
  endTurn();
  return links;
}

const statement = (maximum: number) =>
  z
    .string()
    .trim()
    .refine((value) => [...value].length >= 1 && [...value].length <= maximum);
const assignmentDraft = z
  .object({
    objective: statement(4096),
    success_criteria: statement(4096),
    due_text: statement(120).nullable().default(null),
    due_at: z.iso.datetime({ offset: true }).nullable().default(null),
  })
  .strict();
export type AssignmentDraft = z.infer<typeof assignmentDraft>;

/** A passive proposal artifact. It carries no saved Work identity or authority. */
export function extractAssignmentCard(
  message: Message,
): AssignmentDraft | null {
  if (message.type !== "tool" || message.name !== "propose_assignment")
    return null;
  const artifact: unknown = Reflect.get(message, "artifact");
  const card = isRecord(artifact) ? artifact.assignment_card : null;
  if (
    !isRecord(card) ||
    card.version !== 1 ||
    card.kind !== "assignment_card" ||
    typeof card.tool_call_id !== "string" ||
    !card.tool_call_id ||
    card.tool_call_id !== message.tool_call_id
  )
    return null;
  const parsed = assignmentDraft.safeParse(card.draft);
  return parsed.success ? parsed.data : null;
}
