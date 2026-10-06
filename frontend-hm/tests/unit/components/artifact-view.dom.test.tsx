import { afterEach, describe, expect, it, rs } from "@rstest/core";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { StrictMode } from "react";

import { ArtifactView } from "@/components/workspace/artifacts/artifact-view";
import { parseArtifactView } from "@/core/artifact-views/contract";
import { ArtifactImageSession } from "@/core/artifact-views/images";
import { createEnUS } from "@/core/i18n/locales/en-US";

const save = rs.fn(async () => undefined);
const share = rs.fn(async () => undefined);
rs.mock("@/core/i18n/hooks", () => ({
  useI18n: () => ({ t: createEnUS("HartMesh") }),
}));
rs.mock("@/core/files", () => ({
  useSaveToMyFiles: () => ({ save, isPending: false }),
}));
rs.mock("@/core/shared", () => ({
  useShareWithEveryone: () => ({ share, isPending: false }),
}));
rs.mock("@/core/artifact-views/exports", () => ({
  useLiveArtifactExports: ({
    eligible,
  }: {
    eligible: { path: string; label: string }[];
  }) => ({
    files: eligible,
    settled: true,
    uncertain: false,
    checking: false,
    retry: () => undefined,
  }),
}));

const path = "/mnt/user-data/outputs/results/summary.view.json";
const pdf = "/mnt/user-data/outputs/results/summary.pdf";
const docx = "/mnt/user-data/outputs/results/summary.docx";
const document = parseArtifactView(
  JSON.stringify({
    format: "hartmesh.artifact-view",
    version: 1,
    title: "Document summary",
    accent: "#aabbcc",
    destination: { collection: "Summaries" },
    primary_source: { path: "source.json" },
    blocks: [
      {
        type: "text",
        heading: "Scope",
        paragraphs: ["<script>literal text</script>"],
      },
      { type: "facts", items: [{ label: "Total", value: "$52,310.40" }] },
      {
        type: "notice",
        tone: "positive",
        text: "Producer-authored observation",
        attribution: "Summary skill",
      },
    ],
    exports: [
      { path: "summary.pdf", label: "Printable summary" },
      { path: "summary.docx", label: "Editable summary" },
      { path: "not-presented.xlsx", label: "Not presented" },
    ],
  }),
)!;

describe("generic passive result rendering", () => {
  afterEach(() => {
    cleanup();
    rs.restoreAllMocks();
    rs.unstubAllGlobals();
    save.mockClear();
    share.mockClear();
  });

  const imageView = {
    ...document,
    blocks: [{ type: "image" as const, path: "pixel.png", alt: "Local pixel" }],
    exports: [],
  };
  const imageProps = {
    view: imageView,
    filepath: path,
    threadId: "thread-1",
    revision: "a".repeat(64),
    viewerId: "viewer-1",
    artifacts: [],
    presentedKnown: true,
    runSettled: true,
  };
  const PNG = Uint8Array.from(
    Buffer.from(
      "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGP4z8DwHwAFAAH/iZk9HQAAAABJRU5ErkJggg==",
      "base64",
    ),
  );

  function rasterEnvironment() {
    const create = rs.fn(() => "blob:current-view");
    const revoke = rs.fn();
    rs.stubGlobal(
      "URL",
      class extends URL {
        static createObjectURL = create;
        static revokeObjectURL = revoke;
      },
    );
    const fetch = rs
      .spyOn(globalThis, "fetch")
      .mockImplementation(async () => new Response(PNG));
    const close = rs.fn();
    const decode = rs.fn().mockResolvedValue({ width: 1, height: 1, close });
    rs.stubGlobal("createImageBitmap", decode);
    return { create, revoke, fetch, close, decode };
  }

  it("owns a live image after real StrictMode replay and revokes it on unmount", async () => {
    const { create, revoke, close } = rasterEnvironment();
    const dispose = rs.spyOn(ArtifactImageSession.prototype, "dispose");
    const { unmount } = render(
      <StrictMode>
        <ArtifactView {...imageProps} />
      </StrictMode>,
    );
    await waitFor(() =>
      expect(screen.getByAltText("Local pixel").getAttribute("src")).toBe(
        "blob:current-view",
      ),
    );
    expect(dispose).toHaveBeenCalledTimes(1);
    expect(create).toHaveBeenCalledTimes(1);
    expect(close).toHaveBeenCalledTimes(1);
    unmount();
    expect(dispose).toHaveBeenCalledTimes(2);
    expect(revoke).toHaveBeenCalledExactlyOnceWith("blob:current-view");
  });

  it.each(["viewerId", "threadId", "filepath", "revision"] as const)(
    "retires a late previous %s image without replacing the current view",
    async (field) => {
      const { create, revoke, decode } = rasterEnvironment();
      let finish:
        | ((bitmap: {
            width: number;
            height: number;
            close: () => void;
          }) => void)
        | undefined;
      const oldClose = rs.fn();
      decode.mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            finish = resolve;
          }),
      );
      const dispose = rs.spyOn(ArtifactImageSession.prototype, "dispose");
      const { rerender, unmount } = render(<ArtifactView {...imageProps} />);
      await waitFor(() => expect(decode).toHaveBeenCalledTimes(1));
      const changed = {
        ...imageProps,
        [field]:
          field === "revision"
            ? "b".repeat(64)
            : field === "filepath"
              ? "/mnt/user-data/outputs/other/summary.view.json"
              : "replacement",
      };
      rerender(<ArtifactView {...changed} />);
      await waitFor(() =>
        expect(screen.getByAltText("Local pixel").getAttribute("src")).toBe(
          "blob:current-view",
        ),
      );
      expect(dispose).toHaveBeenCalledTimes(1);
      finish!({ width: 1, height: 1, close: oldClose });
      await waitFor(() => expect(oldClose).toHaveBeenCalledTimes(1));
      expect(create).toHaveBeenCalledTimes(1);
      expect(screen.getByAltText("Local pixel").getAttribute("src")).toBe(
        "blob:current-view",
      );
      unmount();
      expect(revoke).toHaveBeenCalledExactlyOnceWith("blob:current-view");
    },
  );

  it("renders authored values literally and adds no platform verification badge", () => {
    const { container } = render(
      <ArtifactView
        view={document}
        filepath={path}
        threadId="thread-1"
        revision={"a".repeat(64)}
        artifacts={[path, pdf, docx]}
        presentedKnown
        runSettled
      />,
    );
    expect(screen.getByText("<script>literal text</script>")).toBeDefined();
    expect(container.querySelector("script")).toBeNull();
    expect(screen.getByText("$52,310.40")).toBeDefined();
    expect(screen.queryByText(/verified/i)).toBeNull();
    expect(
      screen.getByText("Save and share destination: Summaries"),
    ).toBeDefined();
    expect(screen.queryByText("Not presented")).toBeNull();
  });

  it("saves and shares only explicitly selected eligible exports", async () => {
    render(
      <ArtifactView
        view={document}
        filepath={path}
        threadId="thread-1"
        revision={"a".repeat(64)}
        artifacts={[path, pdf, docx]}
        presentedKnown
        runSettled
      />,
    );
    fireEvent.click(screen.getByRole("checkbox", { name: "Editable summary" }));
    fireEvent.click(screen.getByRole("button", { name: "Save to My files" }));
    expect(save).toHaveBeenCalledWith([pdf], "Summaries");
    fireEvent.click(
      screen.getByRole("button", { name: "Share with everyone" }),
    );
    expect(share).toHaveBeenCalledWith([pdf], "Summaries");
  });

  it("does not make an empty-file claim before presentation history is known", () => {
    render(
      <ArtifactView
        view={document}
        filepath={path}
        threadId="thread-1"
        revision={"a".repeat(64)}
        artifacts={[]}
        presentedKnown={false}
        runSettled
      />,
    );
    expect(screen.getByText("Checking file availability...")).toBeDefined();
    expect(
      screen.queryByText("No presented download is currently available."),
    ).toBeNull();
  });
});
