import { afterEach, beforeEach, describe, expect, it, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";

const myFilesSave = rs.hoisted(() =>
  rs.fn<(paths: readonly string[], folder?: string) => Promise<unknown[]>>(),
);
const shareWithEveryone = rs.hoisted(() =>
  rs.fn<(paths: readonly string[], folder?: string) => Promise<unknown[]>>(),
);
const artifacts = rs.hoisted(() => ({
  list: [] as string[],
  recorded: [] as string[],
}));
const extensionState = rs.hoisted(() => ({
  entries: [] as LoadedContribution[],
  content: undefined as string | undefined,
  projected: false,
  truncated: false,
  loadFull: rs.fn(),
}));

rs.mock("sonner", () => ({ toast: { success: rs.fn(), error: rs.fn() } }));
rs.mock("@/core/extensions/hooks", () => ({
  useFrontendExtensions: () => ({ data: extensionState.entries }),
  useFrontendServices: () => ({
    conversationText: rs.fn(),
    showMessage: rs.fn(),
  }),
}));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({ user: { id: "viewer-1", system_role: "user" } }),
}));
// The hooks are stood in for; the filing rules stay real, so what the panel
// passes is what the product would pass.
rs.mock("@/core/files/hooks", () => ({
  useSaveToMyFiles: () => ({ save: myFilesSave, isPending: false }),
}));
rs.mock("@/core/shared/hooks", () => ({
  useShareWithEveryone: () => ({
    share: shareWithEveryone,
    isPending: false,
    openShared: rs.fn(),
  }),
}));
rs.mock("@/core/artifacts/hooks", () => ({
  useArtifactContent: () => ({
    content: extensionState.content,
    sha256: "a".repeat(64),
    projected: extensionState.projected,
    truncated: extensionState.truncated,
    loadFullContent: extensionState.loadFull,
    isLoading: false,
    data: { content: "", size: 12, is_binary: true },
    isPending: false,
    error: null,
  }),
}));
rs.mock("@/components/workspace/artifacts/context", () => ({
  useArtifacts: () => ({
    artifacts: artifacts.list,
    setOpen: rs.fn(),
    select: rs.fn(),
    drafts: {},
    setDrafts: rs.fn(),
    editingPath: null,
    setEditingPath: rs.fn(),
  }),
}));
rs.mock("@/components/workspace/messages/context", () => ({
  useThread: () => ({
    thread: {
      isLoading: false,
      messages: [
        {
          type: "tool",
          additional_kwargs: { presented_files: artifacts.recorded },
        },
      ],
    },
    isMock: false,
  }),
}));

import { ArtifactFileDetail } from "@/components/workspace/artifacts/artifact-file-detail";
import type { LoadedContribution } from "@/core/extensions/registry";
import { I18nContext } from "@/core/i18n/context";
import { enUS } from "@/core/i18n/locales/en-US";

import { legacyReportContribution } from "../../../../helpers/legacy-report";

const DIRECTORY = "/mnt/user-data/outputs/reports/2026-08-business-review";
const REPORT = `${DIRECTORY}/2026-08-business-review.report.json`;
const RENDER = `${DIRECTORY}/2026-08-business-review.pdf`;

function renderPanel(filepath: string) {
  return render(
    <QueryClientProvider
      client={
        new QueryClient({
          defaultOptions: { queries: { retry: false, gcTime: 0 } },
        })
      }
    >
      <I18nContext.Provider
        value={{ locale: "en-US", setLocale: () => undefined, t: enUS }}
      >
        <ArtifactFileDetail filepath={filepath} threadId="thread-1" />
      </I18nContext.Provider>
    </QueryClientProvider>,
  );
}

describe("ArtifactFileDetail filing", () => {
  beforeEach(() => {
    myFilesSave.mockReset();
    myFilesSave.mockResolvedValue([]);
    shareWithEveryone.mockReset();
    shareWithEveryone.mockResolvedValue([]);
    artifacts.list = [REPORT, RENDER];
    extensionState.entries = [];
    extensionState.content = undefined;
    extensionState.projected = false;
    extensionState.truncated = false;
    extensionState.loadFull.mockReset();
    artifacts.recorded = [];
  });

  afterEach(() => {
    cleanup();
    rs.restoreAllMocks();
  });

  it("files a report's download with the reports, as the report card does", () => {
    // Same file, another way in: the panel must not put a second copy of one
    // report at the root while the card files it under Reports.
    extensionState.entries = [legacyReportContribution()];
    artifacts.recorded = [REPORT, RENDER];
    renderPanel(RENDER);

    fireEvent.click(screen.getByRole("button", { name: "Save to My files" }));
    expect(myFilesSave).toHaveBeenCalledWith([RENDER], "Reports");

    fireEvent.click(
      screen.getByRole("button", { name: "Share with everyone" }),
    );
    expect(shareWithEveryone).toHaveBeenCalledWith([RENDER], "Reports");
  });

  it("leaves an ordinary file of the conversation at the root", () => {
    const notes = "/mnt/user-data/outputs/notes.txt";
    artifacts.list = [notes];
    renderPanel(notes);

    fireEvent.click(
      screen.getByRole("button", { name: "Share with everyone" }),
    );
    expect(shareWithEveryone).toHaveBeenCalledWith([notes], undefined);
  });

  it.each([".pdf", ".opaque"])(
    "lets an installed renderer own a non-code %s preview",
    (suffix) => {
      const source = `/mnt/user-data/outputs/sample${suffix}`;
      artifacts.list = [source];
      extensionState.content = '{"summary":"Projected binary summary"}';
      extensionState.projected = true;
      extensionState.entries = [
        {
          namespace: "example.summary",
          viewer_id: "viewer-1",
          module: "summary.v1",
          entry: "installed-entry",
          title: "Summary",
          description: "",
          settings: { enabled: true },
          artifact_presentations: [
            {
              id: "summary",
              suffixes: [suffix],
              source_max_bytes: 4096,
              preview_max_bytes: 1024,
              projection_marker: "example-summary-v1",
            },
          ],
          extension: {
            apiVersion: 1,
            module: "summary.v1",
            artifactApiVersion: 1,
            artifacts: [
              {
                id: "summary",
                title: "Summary",
                kind: "native",
                mount(root, context) {
                  root.textContent = (
                    JSON.parse(context.artifact.content) as { summary: string }
                  ).summary;
                  return {
                    dispose() {
                      root.replaceChildren();
                    },
                  };
                },
              },
            ],
          },
        },
      ];
      const { container } = renderPanel(source);
      expect(
        screen.getByTestId("plugin-artifact-presentation").firstElementChild
          ?.shadowRoot?.textContent,
      ).toBe("Projected binary summary");
      expect(container.querySelectorAll("iframe")).toHaveLength(0);
      expect(screen.queryAllByRole("radio")).toHaveLength(0);
      expect(
        screen.queryByText(
          "This file type cannot be previewed in the browser.",
        ),
      ).toBeNull();
    },
  );
  it("opens a unique recorded sibling view from the source and preserves canonical source controls", async () => {
    const source = "/mnt/user-data/outputs/results/source.json";
    const viewPath = "/mnt/user-data/outputs/results/result.view.json";
    artifacts.list = [source, viewPath];
    artifacts.recorded = [source, viewPath];
    extensionState.content = '{"source":"Original source"}';
    rs.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(
        JSON.stringify({
          format: "hartmesh.artifact-view",
          version: 1,
          title: "Preferred result",
          primary_source: { path: "source.json" },
          blocks: [],
          exports: [],
        }),
        { headers: { ETag: `"${"b".repeat(64)}"` } },
      ),
    );
    renderPanel(source);
    await waitFor(() =>
      expect(
        screen.getByRole("heading", { name: "Preferred result" }),
      ).toBeDefined(),
    );
    fireEvent.click(
      screen.getByRole("button", { name: enUS.artifactPreview.viewSource }),
    );
    expect(extensionState.loadFull).toHaveBeenCalledTimes(1);
    expect(
      screen.queryByRole("heading", { name: "Preferred result" }),
    ).toBeNull();
  });
  it("renders a complete related view independently of a truncated HTML source", async () => {
    const source = "/mnt/user-data/outputs/results/source.html";
    const viewPath = "/mnt/user-data/outputs/results/result.view.json";
    artifacts.list = [source, viewPath];
    artifacts.recorded = [source, viewPath];
    extensionState.content = "<html><body>Raw source prefix";
    extensionState.truncated = true;
    rs.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(
        JSON.stringify({
          format: "hartmesh.artifact-view",
          version: 1,
          title: "Complete related view",
          primary_source: { path: "source.html" },
          blocks: [],
          exports: [],
        }),
        { headers: { ETag: `"${"b".repeat(64)}"` } },
      ),
    );
    renderPanel(source);
    await waitFor(() =>
      expect(
        screen.getByRole("heading", { name: "Complete related view" }),
      ).toBeDefined(),
    );
    expect(screen.queryByText(/Showing the first/)).toBeNull();
    fireEvent.click(
      screen.getByRole("button", { name: enUS.artifactPreview.viewSource }),
    );
    expect(extensionState.loadFull).toHaveBeenCalledTimes(1);
    expect(
      screen.queryByRole("heading", { name: "Complete related view" }),
    ).toBeNull();
  });
});
