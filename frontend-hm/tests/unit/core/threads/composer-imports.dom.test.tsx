import { afterEach, expect, test } from "@rstest/core";
import { act, cleanup, renderHook } from "@testing-library/react";

import { enUS } from "@/core/i18n/locales/en-US";
import { zhCN } from "@/core/i18n/locales/zh-CN";
import { useComposerImports } from "@/core/threads/composer-imports";

const note = {
  filename: "notes.txt",
  path: "/mnt/user-data/uploads/notes.txt",
  size: 11,
  status: "uploaded" as const,
};
afterEach(cleanup);
test("admission clears only its own capture, and switching account or destination retires saved files", () => {
  const view = renderHook(
    ({ scope }: { scope: string }) => useComposerImports(scope),
    { initialProps: { scope: "reader/chat-a" } },
  );
  act(() => view.result.current.add(note, "Planning"));
  const first = view.result.current;
  expect(first.chips.map((x) => x.sourceTitle)).toEqual(["Planning"]);
  act(() => first.remove(first.chips));
  act(() => view.result.current.add(note, "Revised plan"));
  act(() => first.remove(first.chips));
  expect(view.result.current.chips.map((x) => x.sourceTitle)).toEqual([
    "Revised plan",
  ]);
  view.rerender({ scope: "reader/chat-b" });
  expect(view.result.current.chips).toEqual([]);
  act(() => first.add(note, "Retired chat"));
  expect(view.result.current.chips).toEqual([]);
  view.rerender({ scope: "reader/chat-a" });
  act(() => first.add(note, "Retired account incarnation"));
  expect(view.result.current.chips).toEqual([]);
});
test("both access descriptions explain what a person with chat access can ask in each language", () => {
  for (const sentence of [
    enUS.agentInstances.access.useHelp("Library"),
    enUS.agentInstances.access.chatOnlyNote("Kim", "Library"),
  ])
    expect(sentence).toContain("ask Library about anything in its files");
  for (const sentence of [
    zhCN.agentInstances.access.useHelp("文库"),
    zhCN.agentInstances.access.chatOnlyNote("小林", "文库"),
  ])
    expect(sentence).toContain("可以向其询问文件中的任何内容");
});
