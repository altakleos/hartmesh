"use client";

import { useLayoutEffect, useRef, useState, type RefObject } from "react";
import { useStickToBottomContext } from "use-stick-to-bottom";

import { ConversationScrollButton } from "@/components/ai-elements/conversation";
import { getMessageRunId } from "@/core/messages/run-duration";
import {
  extractTextFromMessage,
  stripUploadedFilesTag,
  type MessageGroup,
} from "@/core/messages/utils";
import type { SentTurn } from "@/core/threads/hooks";

export const GROUP_START_OFFSET = 16;

export function sentGroupIndex(
  groups: readonly MessageGroup[],
  turn?: SentTurn | null,
) {
  if (!turn?.runId) return -1;
  const optimistic = groups.findIndex(
    (g) => g.type === "human" && g.id === turn.optimisticId,
  );
  if (optimistic >= 0) return optimistic;
  const previous = turn.previousHumanId
    ? groups.findIndex(
        (g) => g.type === "human" && g.id === turn.previousHumanId,
      )
    : -1;
  // When the previous boundary is absent, only an explicit run identity can
  // identify the new row (e.g. during a history refresh).
  const firstCandidate = groups.findIndex(
    (group, i) => group.type === "human" && i > previous,
  );
  const explicit = groups.findIndex(
    (group, i) =>
      i > previous &&
      group.type === "human" &&
      getMessageRunId(group.messages[0]!) === turn.runId,
  );
  if (explicit >= 0) return explicit;
  if (previous < 0 && turn.previousHumanId !== null) return -1;
  const first = groups[firstCandidate]?.messages[0];
  return first &&
    !getMessageRunId(first) &&
    stripUploadedFilesTag(extractTextFromMessage(first)) ===
      stripUploadedFilesTag(turn.text)
    ? firstCandidate
    : -1;
}

export type SentScrollControl = {
  holding: boolean;
  clear?: () => void;
};

const scrollInputs = ["wheel", "pointerdown", "touchstart"] as const;
const scrollKeys = new Set([
  "ArrowUp",
  "ArrowDown",
  "ArrowLeft",
  "ArrowRight",
  "PageUp",
  "PageDown",
  "Home",
  "End",
  " ",
]);

export function SentMessageScroll({
  turn,
  groupIndex,
  groupId,
  loading,
  footer,
  control,
  label,
  onRelease,
}: {
  turn: SentTurn;
  groupIndex: number;
  groupId?: string;
  loading: boolean;
  footer: RefObject<HTMLDivElement | null>;
  control: RefObject<SentScrollControl>;
  label: string;
  onRelease?: (optimisticId: string) => void;
}) {
  const { scrollRef, contentRef, stopScroll } = useStickToBottomContext();
  const [showJump, setShowJump] = useState<boolean | undefined>();
  const current = useRef<
    ((index: number, loading: boolean) => void) | undefined
  >(undefined);

  useLayoutEffect(() => {
    const viewport = scrollRef.current;
    const content = contentRef.current;
    const end = footer.current;
    const handle = control.current;
    if (!viewport || !content || !end) {
      setShowJump(undefined);
      return;
    }
    let phase: "pending" | "pinned" | "settled" | "released" | "closed" =
      "pending";
    let row: HTMLElement | null = null;
    let settleFrame = 0;
    let extra = 0;
    let ended = false;

    const reserve = (height: number) => {
      // The stick-to-bottom observer clamps scrollTop one pixel inside the
      // scroll range. Leave it room when retaining a short reply.
      const next = height > 0 ? Math.ceil(height) + 2 : 0;
      if (next === extra) return;
      extra = next;
      end.style.marginBottom = `${next}px`;
    };
    const measure = () => {
      if (phase === "closed" || phase === "pending") return;
      const bounds = viewport.getBoundingClientRect();
      const naturalBottom = content.getBoundingClientRect().bottom - extra;
      const realBottom =
        end.previousElementSibling?.getBoundingClientRect().bottom ??
        naturalBottom;
      const rowBounds = row?.getBoundingClientRect();
      if (
        phase === "pinned" &&
        rowBounds &&
        rowBounds.height > viewport.clientHeight
      ) {
        clear();
        return;
      }
      if (phase === "pinned" && rowBounds && row?.isConnected) {
        const delta = rowBounds.top - bounds.top - GROUP_START_OFFSET;
        reserve(viewport.clientHeight - rowBounds.height - GROUP_START_OFFSET);
        if (Math.abs(delta) > 0.5) {
          viewport.scrollTo({
            top: viewport.scrollTop + delta,
            behavior: "instant",
          });
        }
      } else if (phase !== "pinned") {
        // Keep precisely the space the current position needs. A short reply
        // can finish or the reader can scroll upward without a layout jump.
        reserve(bounds.top + viewport.clientHeight - naturalBottom);
      }
      handle.holding = phase === "pinned" || extra > 0;
      if (!handle.holding) {
        clear();
        return;
      }
      setShowJump(realBottom - bounds.top - viewport.clientHeight > 2);
    };
    function clear() {
      phase = "closed";
      onRelease?.(turn.optimisticId);
      cancelAnimationFrame(settleFrame);
      reserve(0);
      handle.holding = false;
      setShowJump(undefined);
    }
    const release = () => {
      if (phase === "closed" || phase === "released") return;
      if (phase === "pending") {
        clear();
        return;
      }
      phase = "released";
      onRelease?.(turn.optimisticId);
      cancelAnimationFrame(settleFrame);
      stopScroll();
      measure();
    };
    const onKey = (event: KeyboardEvent) => {
      if (scrollKeys.has(event.key)) release();
    };
    const observer = new ResizeObserver(measure);
    observer.observe(viewport);
    observer.observe(content);
    const update = (index: number, isLoading: boolean) => {
      if (phase === "closed" || phase === "released") return;
      const next =
        index < 0
          ? null
          : content.querySelector<HTMLElement>(
              `[data-message-group-index="${index}"]`,
            );
      if (next && next !== row) {
        phase = "pinned";
        row = next;
        if (row.getBoundingClientRect().height > viewport.clientHeight) {
          clear();
          return;
        }
        stopScroll();
        handle.holding = true;
        measure();
        cancelAnimationFrame(settleFrame);
        settleFrame = requestAnimationFrame(() => {
          if (phase !== "pinned") return;
          measure();
          settleFrame = requestAnimationFrame(measure);
        });
      }
      if (row && !isLoading) ended = true;
      if (phase === "pinned" && ended) {
        phase = "settled";
        measure();
      }
    };
    current.current = update;
    handle.clear = clear;
    viewport.addEventListener("scroll", measure, { passive: true });
    for (const event of scrollInputs)
      viewport.addEventListener(event, release, {
        passive: true,
        capture: true,
      });
    document.addEventListener("keydown", onKey, true);
    return () => {
      observer.disconnect();
      cancelAnimationFrame(settleFrame);
      for (const event of scrollInputs)
        viewport.removeEventListener(event, release, true);
      viewport.removeEventListener("scroll", measure);
      document.removeEventListener("keydown", onKey, true);
      current.current = undefined;
      handle.clear = undefined;
      handle.holding = false;
      reserve(0);
    };
  }, [
    turn.optimisticId,
    footer,
    control,
    scrollRef,
    contentRef,
    stopScroll,
    onRelease,
  ]);

  useLayoutEffect(() => {
    current.current?.(groupIndex, loading);
  }, [turn, groupIndex, groupId, loading]);
  return (
    <ConversationScrollButton
      aria-label={label}
      className="z-10"
      visible={showJump}
      onBeforeScroll={() => control.current.clear?.()}
    />
  );
}
