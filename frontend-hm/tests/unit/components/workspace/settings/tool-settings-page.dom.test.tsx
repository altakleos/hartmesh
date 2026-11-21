import { afterEach, describe, expect, it, rs } from "@rstest/core";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
} from "@testing-library/react";

import { ToolSettingsPage } from "@/components/workspace/settings/tool-settings-page";
import { enUS } from "@/core/i18n/locales/en-US";

const mcpMockState = rs.hoisted(() => ({
  check: rs.fn(),
  actor: "operator",
  localMcpManagement: true,
  isPending: false,
  mutate: rs.fn(),
  updateIsPending: false,
  updateMutate: rs.fn(),
  servers: {} as Record<string, unknown>,
}));
// A server carrying config this page never renders: it must survive a write
// that only meant to add or remove some other entry.
const DURABLE_TASK_SERVER = {
  enabled: true,
  description: "Remote tools",
  type: "http",
  url: "https://example.test/mcp",
  task_toolsets: [{ submit: "run", status: "poll" }],
  routing: { mode: "prefer", priority: 50 },
  headers: { "X-API-Key": "***" },
};

rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({ user: { id: mcpMockState.actor, system_role: "admin" } }),
}));
rs.mock("@/core/mcp/api", () => ({
  MCPConfigRequestError: Error,
  checkMCPConnection: mcpMockState.check,
}));
rs.mock("@/core/i18n/hooks", () => ({
  useI18n: () => ({
    t: {
      common: {
        loading: "Loading",
        cancel: "Cancel",
        save: "Save",
        delete: "Delete",
        edit: "Edit",
      },
      settings: {
        providerEnablement: "Customization requires provider enablement.",
        tools: {
          connection: enUS.settings.tools.connection,
          title: "Tools",
          description: "Manage MCP tools",
          adminRequired: "Admin required",
          empty: "No tools",
          addServer: "Add server",
          addServerDescription: "Paste the definition",
          addServerPlaceholder: "{}",
          serverDefinitionLabel: "MCP server JSON definition",
          definitionEmpty: "Paste a definition",
          definitionInvalidJson: "Enter valid JSON",
          definitionRootNotObject: "Enter a JSON object",
          definitionNoServers: "No server found",
          definitionServerNotObject:
            'The configuration for server "{name}" must be an object',
          editServer: "Edit MCP server",
          editServerDescription: 'Edit "{name}"',
          editSingleServer: "Edit exactly one server",
          editServerNameMismatch: 'Keep the name "{name}"',
          serverAlreadyExists: 'Server "{name}" already exists',
          removeServer: "Remove MCP server",
          removeServerDescription: 'Remove "{name}"?',
          unnamedServer: "(empty name)",
        },
      },
    },
  }),
}));

rs.mock("@/core/features/hooks", () => ({
  useCustomerAdministration: () => ({
    localMcpManagement: mcpMockState.localMcpManagement,
  }),
  useBranding: () => ({ providerName: null, supportURL: null }),
}));

rs.mock("@/core/mcp/hooks", () => ({
  useMCPConfig: () => ({
    config: { mcp_servers: mcpMockState.servers },
    isLoading: false,
    error: null,
  }),
  useEnableMCPServer: () => ({
    isPending: mcpMockState.isPending,
    mutate: mcpMockState.mutate,
  }),
  useMCPServerMutation: () => ({
    isPending: mcpMockState.updateIsPending,
    mutate: mcpMockState.updateMutate,
  }),
}));

rs.mock("@/env", () => ({
  env: { NEXT_PUBLIC_STATIC_WEBSITE_ONLY: "false" },
}));

function setServers(servers: Record<string, unknown>) {
  mcpMockState.servers = servers;
}

function twoServers() {
  setServers({
    github: { enabled: true, description: "GitHub tools" },
    remote: { ...DURABLE_TASK_SERVER, enabled: false },
  });
}

/** The targeted mutation variables handed to the last call. */
function lastMutation() {
  const call = mcpMockState.updateMutate.mock.calls.at(-1);
  return call?.[0] as Record<string, unknown>;
}

function openAddDialog() {
  fireEvent.click(screen.getByRole("button", { name: "Add connection" }));
  fireEvent.click(
    screen.getByRole("button", { name: "Paste a JSON definition instead" }),
  );
}

function openEditDialog(name: string) {
  fireEvent.click(screen.getByRole("button", { name: `Edit ${name}` }));
}

function definitionTextbox(): HTMLTextAreaElement {
  const element = screen.getByRole("textbox");
  if (!(element instanceof HTMLTextAreaElement)) {
    throw new TypeError("MCP definition editor must be a textarea");
  }
  return element;
}

afterEach(() => {
  mcpMockState.localMcpManagement = true;
  mcpMockState.actor = "operator";
  mcpMockState.check.mockReset();
  mcpMockState.isPending = false;
  mcpMockState.updateIsPending = false;
  mcpMockState.mutate.mockReset();
  mcpMockState.updateMutate.mockReset();
  mcpMockState.servers = {};
  cleanup();
});

describe("ToolSettingsPage MCP switches", () => {
  it("disables every switch while a targeted update is pending", () => {
    twoServers();
    mcpMockState.isPending = true;

    render(<ToolSettingsPage />);

    const switches = screen.getAllByRole("switch");
    expect(switches).toHaveLength(2);
    for (const item of switches) {
      expect((item as HTMLButtonElement).disabled).toBe(true);
    }
  });

  it("submits only the selected server state when idle", () => {
    twoServers();

    render(<ToolSettingsPage />);

    const switches = screen.getAllByRole("switch");
    const githubSwitch = switches[0];
    expect(githubSwitch).toBeDefined();
    expect((githubSwitch as HTMLButtonElement).disabled).toBe(false);

    fireEvent.click(githubSwitch!);

    expect(mcpMockState.mutate).toHaveBeenCalledWith({
      serverName: "github",
      enabled: false,
    });
  });
});

describe("ToolSettingsPage add server", () => {
  it("submits only the pasted servers to the atomic create endpoint", () => {
    twoServers();

    render(<ToolSettingsPage />);
    openAddDialog();
    fireEvent.change(screen.getByRole("textbox"), {
      target: {
        value: '{"mcpServers": {"added": {"command": "npx", "args": []}}}',
      },
    });
    fireEvent.click(screen.getByRole("button", { name: "Save" }));

    expect(lastMutation()).toEqual({
      operation: "create",
      servers: {
        added: { command: "npx", args: [], enabled: true, description: "" },
      },
    });
  });

  it("does not submit stale sibling configurations while adding", () => {
    twoServers();

    render(<ToolSettingsPage />);
    openAddDialog();
    fireEvent.change(screen.getByRole("textbox"), {
      target: { value: '{"added": {"command": "uvx"}}' },
    });
    fireEvent.click(screen.getByRole("button", { name: "Save" }));

    expect(lastMutation()).toEqual({
      operation: "create",
      servers: {
        added: { command: "uvx", enabled: true, description: "" },
      },
    });
  });

  it("reports a malformed definition without writing", () => {
    twoServers();

    render(<ToolSettingsPage />);
    openAddDialog();
    fireEvent.change(screen.getByRole("textbox"), {
      target: { value: "{not json" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Save" }));

    expect(mcpMockState.updateMutate).not.toHaveBeenCalled();
    expect(screen.getByRole("alert").textContent).toBe("Enter valid JSON");
    expect(
      screen.getByRole("textbox", { name: "MCP server JSON definition" }),
    ).toBeDefined();
  });

  it("offers the add action when no server is configured yet", () => {
    setServers({});

    render(<ToolSettingsPage />);

    expect(screen.getByText("No tools")).toBeDefined();
    expect(
      screen
        .getByRole("button", { name: "Add connection" })
        .hasAttribute("disabled"),
    ).toBe(false);
  });

  it("rejects an existing name instead of silently replacing it", () => {
    twoServers();

    render(<ToolSettingsPage />);
    openAddDialog();
    fireEvent.change(screen.getByRole("textbox"), {
      target: { value: '{"github": {"command": "uvx"}}' },
    });
    fireEvent.click(screen.getByRole("button", { name: "Save" }));

    expect(mcpMockState.updateMutate).not.toHaveBeenCalled();
    expect(screen.getByText('Server "github" already exists')).toBeDefined();
  });
});

describe("ToolSettingsPage edit server", () => {
  it("prefills the complete server definition", () => {
    twoServers();

    render(<ToolSettingsPage />);
    openEditDialog("remote");

    const definition = JSON.parse(definitionTextbox().value) as {
      mcpServers: Record<string, unknown>;
    };
    expect(Object.keys(definition.mcpServers)).toEqual(["remote"]);
    expect(definition.mcpServers.remote).toEqual({
      ...DURABLE_TASK_SERVER,
      enabled: false,
    });
  });

  it("updates only one server while preserving all of its hidden fields", () => {
    twoServers();

    render(<ToolSettingsPage />);
    openEditDialog("remote");
    const textbox = definitionTextbox();
    const definition = JSON.parse(textbox.value) as {
      mcpServers: Record<string, Record<string, unknown>>;
    };
    definition.mcpServers.remote!.description = "Updated remote tools";
    fireEvent.change(textbox, {
      target: { value: JSON.stringify(definition) },
    });
    fireEvent.click(screen.getByRole("button", { name: "Save" }));

    expect(lastMutation()).toEqual({
      operation: "update",
      serverName: "remote",
      server: {
        ...DURABLE_TASK_SERVER,
        enabled: false,
        description: "Updated remote tools",
      },
    });
  });

  it("rejects renaming through the edit dialog", () => {
    twoServers();

    render(<ToolSettingsPage />);
    openEditDialog("github");
    fireEvent.change(screen.getByRole("textbox"), {
      target: {
        value: '{"mcpServers": {"renamed": {"command": "npx"}}}',
      },
    });
    fireEvent.click(screen.getByRole("button", { name: "Save" }));

    expect(mcpMockState.updateMutate).not.toHaveBeenCalled();
    expect(screen.getByText('Keep the name "github"')).toBeDefined();
  });

  it("rejects editing multiple servers at once", () => {
    twoServers();

    render(<ToolSettingsPage />);
    openEditDialog("github");
    fireEvent.change(screen.getByRole("textbox"), {
      target: {
        value:
          '{"mcpServers": {"github": {"command": "npx"}, "extra": {"command": "uvx"}}}',
      },
    });
    fireEvent.click(screen.getByRole("button", { name: "Save" }));

    expect(mcpMockState.updateMutate).not.toHaveBeenCalled();
    expect(screen.getByText("Edit exactly one server")).toBeDefined();
  });
});

describe("ToolSettingsPage remove server", () => {
  it("submits only the selected server name", () => {
    twoServers();

    render(<ToolSettingsPage />);
    fireEvent.click(screen.getByRole("button", { name: "Delete github" }));
    fireEvent.click(screen.getByRole("button", { name: "Delete" }));

    expect(lastMutation()).toEqual({
      operation: "delete",
      serverName: "github",
    });
  });

  it("does not write when the confirmation is dismissed", () => {
    twoServers();

    render(<ToolSettingsPage />);
    fireEvent.click(screen.getByRole("button", { name: "Delete github" }));
    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));

    expect(mcpMockState.updateMutate).not.toHaveBeenCalled();
  });

  it("deletes a configured server whose name is empty", () => {
    setServers({ "": { enabled: false, description: "Legacy server" } });

    render(<ToolSettingsPage />);
    fireEvent.click(
      screen.getByRole("button", { name: "Delete (empty name)" }),
    );
    fireEvent.click(screen.getByRole("button", { name: "Delete" }));

    expect(lastMutation()).toEqual({
      operation: "delete",
      serverName: "",
    });
  });
});

it("denies local controls without delegation while retaining remote owner preferences", () => {
  twoServers();
  mcpMockState.localMcpManagement = false;
  render(<ToolSettingsPage />);
  function disabled(element: HTMLElement) {
    if (!(element instanceof HTMLButtonElement))
      throw new TypeError("Expected a button");
    return element.disabled;
  }
  const switches = screen.getAllByRole("switch");
  expect(disabled(switches[0]!)).toBe(true);
  expect(disabled(switches[1]!)).toBe(false);
  expect(
    screen.getByText("Customization requires provider enablement."),
  ).toBeTruthy();
  expect(disabled(screen.getByRole("button", { name: "Edit github" }))).toBe(
    true,
  );
  expect(disabled(screen.getByRole("button", { name: "Edit remote" }))).toBe(
    false,
  );
});

it("retains remote transport-alias definitions without local delegation", () => {
  mcpMockState.localMcpManagement = false;
  render(<ToolSettingsPage />);
  openAddDialog();
  fireEvent.change(definitionTextbox(), {
    target: {
      value: JSON.stringify({
        remote: { transport: "http", url: "https://example.test/mcp" },
      }),
    },
  });
  fireEvent.click(screen.getByRole("button", { name: "Save" }));
  expect(lastMutation()?.operation).toBe("create");
});

describe("remote connection forms", () => {
  it("checks only the saved connection, then keeps a refused connection available to edit", async () => {
    mcpMockState.check.mockResolvedValue({
      status: "failed",
      reason: "refused",
    });
    const view = render(<ToolSettingsPage />);
    fireEvent.click(screen.getByRole("button", { name: "Add connection" }));
    fireEvent.change(screen.getByLabelText("Name"), {
      target: { value: "reference" },
    });
    fireEvent.change(screen.getByLabelText("Address"), {
      target: { value: "https://mcp.example.com/notes" },
    });
    fireEvent.change(screen.getByLabelText("Token (optional)"), {
      target: { value: "illustrative" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Save and check" }));
    expect(lastMutation()).toEqual({
      operation: "create",
      servers: {
        reference: {
          enabled: true,
          type: "http",
          url: "https://mcp.example.com/notes",
          headers: { Authorization: "Bearer illustrative" },
          description: "",
        },
      },
    });
    expect(mcpMockState.check).not.toHaveBeenCalled();
    setServers({
      reference: {
        enabled: true,
        type: "http",
        url: "https://mcp.example.com/notes",
        headers: { Authorization: "***" },
        description: "",
      },
    });
    const callbacks = mcpMockState.updateMutate.mock.lastCall?.[1] as {
      onSuccess: (saved: { mcp_servers: Record<string, unknown> }) => void;
    };
    view.rerender(<ToolSettingsPage />);
    await act(async () =>
      callbacks.onSuccess({ mcp_servers: mcpMockState.servers }),
    );
    await screen.findByRole("button", { name: "Edit reference" });
    expect(screen.getByRole("status").textContent).toContain(
      "Saved. The connection was refused.",
    );
    expect(mcpMockState.check).toHaveBeenCalledWith(
      "reference",
      expect.any(AbortSignal),
    );
  });
  it("edits the original header and keeps a switched-off connection, including extra fields", () => {
    const original = {
      enabled: false,
      type: "sse",
      url: "https://mcp.example.com",
      headers: { authorization: "***" },
      tool_name_prefix: false,
      tool_call_timeout: 6,
      session_init_timeout: null,
      extra_field: { v: 1 },
      description: "Before",
    };
    setServers({ "Old name": original });
    render(<ToolSettingsPage />);
    openEditDialog("Old name");
    expect(screen.getByLabelText("Name").hasAttribute("disabled")).toBe(true);
    fireEvent.change(screen.getByLabelText("Description (optional)"), {
      target: { value: "After" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Save and check" }));
    expect(lastMutation()).toEqual({
      operation: "update",
      serverName: "Old name",
      server: { ...original, description: "After" },
    });
  });
  it("prevents a pasted multiline token from silently becoming valid in a password field", () => {
    render(<ToolSettingsPage />);
    fireEvent.click(screen.getByRole("button", { name: "Add connection" }));
    fireEvent.change(screen.getByLabelText("Name"), {
      target: { value: "notes" },
    });
    fireEvent.change(screen.getByLabelText("Address"), {
      target: { value: "https://mcp.example.com" },
    });
    fireEvent.paste(screen.getByLabelText("Token (optional)"), {
      clipboardData: { getData: () => "example\nvalue" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Save and check" }));
    expect(mcpMockState.updateMutate).not.toHaveBeenCalled();
    expect(screen.getByRole("alert").textContent).toContain("line breaks");
  });
  it("retires an old account’s outstanding check without displaying its late result", async () => {
    let settle!: (result: unknown) => void;
    mcpMockState.check.mockImplementation(
      () =>
        new Promise((resolve) => {
          settle = resolve;
        }),
    );
    twoServers();
    const view = render(<ToolSettingsPage />);
    fireEvent.click(screen.getByRole("button", { name: "Check remote" }));
    const signal = mcpMockState.check.mock.lastCall?.[1] as AbortSignal;
    mcpMockState.actor = "other";
    view.rerender(<ToolSettingsPage />);
    expect(signal.aborted).toBe(true);
    await act(async () => settle({ status: "connected", tool_count: 1 }));
    expect(screen.queryByRole("status")).toBeNull();
  });
});

it("retires an open editor when its stored definition changes", () => {
  const old = {
    enabled: true,
    type: "http",
    url: "https://mcp.example.com/v1",
    description: "Original",
  };
  setServers({ notes: old });
  const view = render(<ToolSettingsPage />);
  openEditDialog("notes");
  fireEvent.change(screen.getByLabelText("Description (optional)"), {
    target: { value: "My edit" },
  });
  setServers({ notes: { ...old, url: "https://mcp.example.com/v2" } });
  view.rerender(<ToolSettingsPage />);
  expect(screen.queryByRole("button", { name: "Save and check" })).toBeNull();
  expect(screen.getByRole("alert").textContent).toContain("Open it again");
  expect(mcpMockState.updateMutate).not.toHaveBeenCalled();
});

it.each(["pending", "completed"])(
  "retires the %s diagnostic after a server with the same name changes",
  async (when) => {
    let finish!: (r: unknown) => void;
    mcpMockState.check.mockImplementation(
      () =>
        new Promise((resolve) => {
          finish = resolve;
        }),
    );
    const old = {
      type: "http",
      enabled: true,
      description: "",
      url: "https://mcp.example.com/v1",
      capability_id: "original",
    };
    setServers({ remote: old });
    const view = render(<ToolSettingsPage />);
    fireEvent.click(screen.getByRole("button", { name: "Check remote" }));
    if (when === "completed") {
      await act(async () => finish({ status: "connected", tool_count: 3 }));
      expect(screen.getByRole("status").textContent).toContain(
        "Connected. 3 tools.",
      );
    }
    const signal = mcpMockState.check.mock.lastCall?.[1] as AbortSignal;
    setServers({
      remote: {
        ...old,
        url: "https://mcp.example.com/v2",
        capability_id: "replacement",
      },
    });
    view.rerender(<ToolSettingsPage />);
    expect(signal.aborted).toBe(true);
    if (when === "pending")
      await act(async () => finish({ status: "connected", tool_count: 3 }));
    expect(screen.queryByRole("status")).toBeNull();
    expect(
      screen
        .getByRole("button", { name: "Check remote" })
        .hasAttribute("disabled"),
    ).toBe(false);
  },
);

it("names the loss of edited fields before switching to JSON", () => {
  render(<ToolSettingsPage />);
  fireEvent.click(screen.getByRole("button", { name: "Add connection" }));
  fireEvent.change(screen.getByLabelText("Token (optional)"), {
    target: { value: "example" },
  });
  fireEvent.click(
    screen.getByRole("button", { name: "Discard form changes and open JSON" }),
  );
  expect(definitionTextbox().value).toBe("");
  expect(mcpMockState.updateMutate).not.toHaveBeenCalled();
});

it("shows a saved diagnostic only after the refreshed list matches the returned definition", async () => {
  const old = {
    type: "http",
    enabled: true,
    description: "",
    url: "https://mcp.example.com/v1",
    headers: {},
  };
  const updated = { ...old, description: "Revised" };
  mcpMockState.check.mockResolvedValue({ status: "connected", tool_count: 2 });
  setServers({ notes: old });
  const view = render(<ToolSettingsPage />);
  openEditDialog("notes");
  fireEvent.change(screen.getByLabelText("Description (optional)"), {
    target: { value: "Revised" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Save and check" }));
  const callbacks = mcpMockState.updateMutate.mock.lastCall?.[1] as {
    onSuccess: (r: unknown) => void;
  };
  await act(async () =>
    callbacks.onSuccess({ mcp_servers: { notes: updated } }),
  );
  expect(screen.queryByRole("status")).toBeNull();
  setServers({ notes: updated });
  view.rerender(<ToolSettingsPage />);
  expect(screen.getByRole("status").textContent).toContain(
    "Saved. Connected. 2 tools.",
  );
  setServers({});
  view.rerender(<ToolSettingsPage />);
  setServers({ notes: updated });
  view.rerender(<ToolSettingsPage />);
  expect(screen.queryByRole("status")).toBeNull();
  expect(mcpMockState.check).toHaveBeenCalledTimes(1);
});
