import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { act, cleanup, fireEvent, render } from "@testing-library/react";

import {
  SentMessageScroll,
  sentGroupIndex,
  type SentScrollControl,
} from "@/components/workspace/messages/sent-message-scroll";
import type { MessageGroup } from "@/core/messages/utils";
import type { SentTurn } from "@/core/threads/hooks";

const context = rs.hoisted(() => ({
  scrollRef: { current: null as HTMLDivElement | null },
  contentRef: { current: null as HTMLDivElement | null },
  stopScroll: rs.fn(),
}));
rs.mock("use-stick-to-bottom", () => ({
  useStickToBottomContext: () => context,
}));
rs.mock("@/components/ai-elements/conversation", () => ({
  ConversationScrollButton: () => null,
}));

const attempt = {
  optimisticId: "optimistic",
  previousHumanId: "before",
  text: "Question",
};
const admitted = { ...attempt, runId: "run" };
function human(id: string, text = "Question", run?: string): MessageGroup {
  return {
    type: "human",
    id,
    messages: [
      { id, content: text, type: "human", ...(run ? { run_id: run } : {}) },
    ],
  };
}

it("tracks only a local admission through overlapping, missing and replaced human rows", () => {
  const before = human("before");
  const opt = human("optimistic");
  const server = human("received");
  expect(sentGroupIndex([before, opt], attempt)).toBe(-1);
  expect(sentGroupIndex([before, opt], admitted)).toBe(1);
  expect(sentGroupIndex([before, opt, server], admitted)).toBe(1);
  expect(sentGroupIndex([before], admitted)).toBe(-1);
  expect(
    sentGroupIndex([human("older"), before, server, human("later")], admitted),
  ).toBe(2);
  expect(
    sentGroupIndex([before, human("different", "Another question")], admitted),
  ).toBe(-1);
  expect(sentGroupIndex([human("older"), server], admitted)).toBe(-1);
  expect(
    sentGroupIndex(
      [human("older"), human("confirmed", "Question", "run")],
      admitted,
    ),
  ).toBe(1);
  expect(
    sentGroupIndex([before, human("other", "Question", "other-run")], admitted),
  ).toBe(-1);
  expect(sentGroupIndex([before, server], null)).toBe(-1);
});

it("does not claim old repeated text when the previous human has no stable identity", () => {
  const unknown = { ...admitted, previousHumanId: undefined };
  const old = human("old");
  expect(sentGroupIndex([old], unknown)).toBe(-1);
  expect(
    sentGroupIndex([old, human("receipt", "Question", "run")], unknown),
  ).toBe(1);
  expect(
    sentGroupIndex([human("first")], { ...admitted, previousHumanId: null }),
  ).toBe(0);
});

function view(initial: SentTurn, loading = true) {
  const viewport = document.createElement("div");
  const content = document.createElement("div");
  const row = document.createElement("div");
  row.dataset.messageGroupIndex = "1";
  const end = document.createElement("div");
  viewport.append(content);
  content.append(row, end);
  document.body.append(viewport);
  context.scrollRef.current = viewport;
  context.contentRef.current = content;
  const footer = { current: end };
  const control = { current: { holding: false } as SentScrollControl };
  const rect = (top: number, height: number) =>
    ({ top, height, bottom: top + height }) as DOMRect;
  Object.defineProperty(viewport, "clientHeight", { value: 400 });
  viewport.getBoundingClientRect = () => rect(0, 400);
  const sizeRow = (r: HTMLElement, start: number) => {
    r.getBoundingClientRect = () => rect(start - viewport.scrollTop, 48);
  };
  sizeRow(row, 350);
  content.getBoundingClientRect = () =>
    rect(-viewport.scrollTop, 460 + (parseFloat(end.style.marginBottom) || 0));
  viewport.scrollTo = rs.fn((options: ScrollToOptions) => {
    viewport.scrollTop = options.top ?? 0;
  }) as typeof viewport.scrollTo;
  let resize: () => void = () => undefined;
  rs.stubGlobal(
    "ResizeObserver",
    class {
      constructor(cb: () => void) {
        resize = cb;
      }
      observe = rs.fn();
      disconnect = rs.fn();
    },
  );
  let groupId = "optimistic";
  const ui = (turn: SentTurn, active: boolean) => (
    <SentMessageScroll
      turn={turn}
      groupIndex={turn.runId ? 1 : -1}
      groupId={groupId}
      loading={active}
      footer={footer}
      control={control}
      label="Scroll to bottom"
    />
  );
  const rendered = render(ui(initial, loading));
  return {
    viewport,
    control,
    resize: () => act(resize),
    rerender: (turn: SentTurn, active: boolean) =>
      rendered.rerender(ui(turn, active)),
    replace() {
      const replacement = row.cloneNode() as HTMLElement;
      row.replaceWith(replacement);
      sizeRow(replacement, 385);
      groupId = "received";
    },
  };
}
afterEach(() => {
  cleanup();
  context.scrollRef.current?.remove();
  rs.unstubAllGlobals();
});

describe("sent message viewport ownership", () => {
  it.each([
    "wheel",
    "touchstart",
    "pointerdown",
    "ArrowUp",
    "ArrowDown",
    "ArrowLeft",
    "ArrowRight",
    "PageUp",
    "PageDown",
    " ",
    "Home",
    "End",
  ])("releases on the first %s event", (kind) => {
    const chat = view(admitted);
    expect(chat.viewport.scrollTop).toBe(334);
    if (["wheel", "touchstart", "pointerdown"].includes(kind))
      fireEvent(chat.viewport, new Event(kind, { bubbles: true }));
    else fireEvent.keyDown(document, { key: kind });
    chat.viewport.scrollTop = 200;
    chat.resize();
    expect(chat.viewport.scrollTop).toBe(200);
    chat.replace();
    chat.rerender(admitted, true);
    expect(chat.viewport.scrollTop).toBe(200);
  });
  it("keeps control with the person if they scroll while a send awaits admission", () => {
    const chat = view(attempt);
    fireEvent.wheel(chat.viewport);
    chat.rerender(admitted, true);
    expect(chat.viewport.scrollTop).toBe(0);
  });
  it("settles an immediately complete turn and transfers a late replacement once", () => {
    const chat = view(admitted, false);
    expect(chat.viewport.scrollTop).toBe(334);
    chat.viewport.scrollTop = 300;
    chat.resize();
    expect(chat.viewport.scrollTop).toBe(300);
    chat.replace();
    chat.rerender(admitted, false);
    expect(chat.viewport.scrollTop).toBe(369);
    chat.viewport.scrollTop = 290;
    chat.resize();
    expect(chat.viewport.scrollTop).toBe(290);
  });
});
