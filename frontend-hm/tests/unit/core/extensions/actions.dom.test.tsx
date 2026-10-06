import { afterEach, beforeEach, expect, rs, test } from "@rstest/core";
import {
  cleanup,
  fireEvent,
  act,
  render as renderComponent,
  screen,
} from "@testing-library/react";
import type { ReactNode } from "react";

import { TooltipProvider } from "@/components/ui/tooltip";
import { ConversationExtensionActions } from "@/components/workspace/conversation-extension-actions";
import type { ConversationActionGroup } from "@/core/extensions/contracts";
import type { LoadedContribution } from "@/core/extensions/registry";
import { FileActionLifetimeProvider } from "@/core/file-areas/file-action-lifetime";
import { enUS } from "@/core/i18n";
import type { AgentThread } from "@/core/threads/types";

function render(children: ReactNode) {
  return renderComponent(children, { wrapper: FileActionLifetimeProvider });
}

const messages = rs.hoisted(() => ({
  error: rs.fn(),
  message: rs.fn(),
  dismiss: rs.fn(),
}));
rs.mock("sonner", () => ({ toast: messages }));
const state = rs.hoisted(() => ({ entries: [] as LoadedContribution[] }));
rs.mock("@/core/extensions/hooks", () => ({
  useFrontendExtensions: () => ({ data: state.entries }),
  useFrontendServices: () => ({ showMessage: messages.message }),
}));
rs.mock("@/core/i18n/hooks", () => ({
  useI18n: () => ({ t: enUS, locale: "en-US" }),
}));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "" }));
rs.mock("@/core/api/fetcher", () => ({ fetch: rs.fn() }));

const action = {
  id: "save",
  label: "Save",
  icon: "bookmark",
  available: () => true,
  execute: async () => undefined,
};
const group = { label: "Healthy actions", icon: "bookmark", actions: [action] };
function entry(namespace: string, factory: () => unknown): LoadedContribution {
  return {
    namespace,
    title: namespace,
    description: "",
    module: namespace,
    entry: null,
    settings: { enabled: true },
    extension: {
      apiVersion: 1,
      module: namespace,
      conversationActions: factory as () => ConversationActionGroup,
    },
  };
}
beforeEach(() => {
  rs.clearAllMocks();
  rs.spyOn(console, "warn").mockImplementation(() => undefined);
});

test("retiring a mounted conversation action aborts host services and suppresses late errors", async () => {
  let reject!: (error: Error) => void;
  let bound!: { showMessage: (message: string) => void };
  const execute = rs.fn((_context, services) => {
    bound = services;
    return new Promise<void>((_resolve, fail) => {
      reject = fail;
    });
  });
  state.entries = [
    entry("healthy", () => ({ ...group, actions: [{ ...action, execute }] })),
  ];
  const { unmount } = render(
    <TooltipProvider>
      <ConversationExtensionActions
        context={{ thread: { thread_id: "test" } as AgentThread }}
      />
    </TooltipProvider>,
  );
  fireEvent.pointerDown(
    screen.getByRole("button", { name: "Healthy actions" }),
    { button: 0, ctrlKey: false },
  );
  fireEvent.click(await screen.findByRole("menuitem", { name: "Save" }));
  expect(execute).toHaveBeenCalledTimes(1);
  unmount();
  expect(() => bound.showMessage("late private message")).toThrow();
  await act(async () => {
    reject(new Error("late failure"));
    await Promise.resolve();
  });
  expect(messages.error).not.toHaveBeenCalled();
  expect(messages.message).not.toHaveBeenCalled();
});
afterEach(() => {
  cleanup();
  rs.restoreAllMocks();
});

const invalid: [string, () => unknown][] = [
  [
    "factory throws",
    () => {
      throw new Error("plugin error");
    },
  ],
  ["missing actions", () => ({ label: "Broken", icon: "bookmark" })],
  ["actions is not an array", () => ({ ...group, actions: {} })],
  ["invalid group label", () => ({ ...group, label: {} })],
  ["invalid group icon", () => ({ ...group, icon: {} })],
  [
    "invalid action label",
    () => ({ ...group, actions: [{ ...action, label: {} }] }),
  ],
  ["duplicate action ids", () => ({ ...group, actions: [action, action] })],
  [
    "missing executor",
    () => ({ ...group, actions: [{ ...action, execute: null }] }),
  ],
  [
    "missing availability",
    () => ({ ...group, actions: [{ ...action, available: null }] }),
  ],
  [
    "availability throws",
    () => ({
      ...group,
      actions: [
        {
          ...action,
          available: () => {
            throw new Error("policy failed");
          },
        },
      ],
    }),
  ],
  [
    "availability is asynchronous",
    () => ({ ...group, actions: [{ ...action, available: async () => true }] }),
  ],
];
for (const [name, factory] of invalid) {
  test(`contains ${name} without losing the other plugin or conversation`, () => {
    state.entries = [entry("broken", factory), entry("healthy", () => group)];
    render(
      <TooltipProvider>
        <p>Conversation remains usable</p>
        <ConversationExtensionActions
          context={{ thread: { thread_id: "test" } as AgentThread }}
        />
      </TooltipProvider>,
    );
    expect(screen.getByText("Conversation remains usable")).toBeDefined();
    expect(screen.getAllByRole("button")).toHaveLength(1);
    expect(
      screen.getByRole("button", { name: "Healthy actions" }),
    ).toBeDefined();
  });
}

test("empty, hidden and absent action groups leave healthy actions available", () => {
  state.entries = [
    entry("absent", () => undefined),
    entry("empty", () => ({ ...group, actions: [] })),
    entry("hidden", () => ({
      ...group,
      actions: [{ ...action, available: () => false }],
    })),
    entry("healthy", () => group),
  ];
  render(
    <TooltipProvider>
      <ConversationExtensionActions
        context={{ thread: { thread_id: "test" } as AgentThread }}
      />
    </TooltipProvider>,
  );
  expect(screen.getAllByRole("button")).toHaveLength(1);
  expect(console.warn).not.toHaveBeenCalled();
});

test("render uses validated label snapshots instead of rereading plugin getters", () => {
  let reads = 0;
  state.entries = [
    entry("healthy", () => ({
      ...group,
      get label() {
        if (++reads > 1) throw new Error("getter read twice");
        return "Healthy actions";
      },
    })),
  ];
  render(
    <TooltipProvider>
      <ConversationExtensionActions
        context={{ thread: { thread_id: "test" } as AgentThread }}
      />
    </TooltipProvider>,
  );
  expect(screen.getByRole("button", { name: "Healthy actions" })).toBeDefined();
  expect(reads).toBe(1);
});

test("unknown icon names including prototype properties use the fallback icon", () => {
  state.entries = [
    entry("healthy", () => ({
      ...group,
      icon: "__proto__",
      actions: [{ ...action, icon: "constructor" }],
    })),
  ];
  render(
    <TooltipProvider>
      <ConversationExtensionActions
        context={{ thread: { thread_id: "test" } as AgentThread }}
      />
    </TooltipProvider>,
  );
  expect(screen.getByRole("button", { name: "Healthy actions" })).toBeDefined();
});
