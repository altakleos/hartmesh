import type { Message } from "@langchain/langgraph-sdk";

import type { Translations } from "../i18n";
import {
  isHiddenFromUIMessage,
  isUploadPlaceholderMessage,
} from "../messages/utils";

import type { TurnProgress } from "./types";

/**
 * Whether the streaming turn has shown the person anything of the model's
 * yet: a tool call (its card is on screen, with the model's own description
 * of the step) or answer text. Either ends the window the stage label is for.
 * Scans back to the turn's human message, hidden or not — a slash-command
 * turn's own message is hidden from the list but still starts the turn.
 * The upload placeholder the client draws beside its own message is not
 * the model's: on a cold sandbox it sat there for over ten seconds and hid
 * every stage the run reported.
 */
function turnHasModelOutput(messages: Message[]): boolean {
  for (let index = messages.length - 1; index >= 0; index--) {
    const message = messages[index]!;
    if (message.type === "human") return false;
    if (message.type !== "ai" || isUploadPlaceholderMessage(message)) continue;
    if ((message.tool_calls?.length ?? 0) > 0) return true;
    if (typeof message.content === "string" && message.content.length > 0) {
      return true;
    }
    if (Array.isArray(message.content) && message.content.length > 0) {
      return true;
    }
  }
  return false;
}

/**
 * What the activity row says while the turn runs, or null for "Working…".
 *
 * The stage the run reported (preparing, starting a workspace, thinking)
 * covers the stretch before the model has produced anything, which is where
 * the person had only "Working…" for up to tens of seconds. Once a tool card
 * or answer text is on screen, that content says what is happening and the
 * row goes back to the plain clock: a label repeating the card above it, or
 * "Thinking…" under prose that is already streaming, would be noise or wrong.
 */
export function runActivityLabel({
  isLoading,
  messages,
  progress,
  t,
}: {
  isLoading: boolean;
  messages: Message[];
  progress: TurnProgress | null;
  t: Translations;
}): string | null {
  if (!isLoading || !progress) return null;
  if (
    turnHasModelOutput(
      messages.filter((m) => !isHiddenFromUIMessage(m) || m.type === "human"),
    )
  ) {
    return null;
  }
  switch (progress.stage) {
    case "preparing":
      return t.runProgress.preparing;
    case "workspace_starting":
      return t.runProgress.workspaceStarting;
    case "thinking":
      return t.runProgress.thinking;
    default:
      return null;
  }
}
