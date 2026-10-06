import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import { StrictMode } from "react";

import { PluginArtifactPresentation } from "@/components/workspace/artifacts/plugin-artifact-presentation";
import type { InstalledArtifactPresentation } from "@/core/extensions/artifacts";
import type { ArtifactSurfaceContext } from "@/core/extensions/contracts";
import { createEnUS } from "@/core/i18n/locales/en-US";

rs.mock("@/core/i18n/hooks", () => ({
  useI18n: () => ({ t: createEnUS("HartMesh"), locale: "en-US" }),
}));
rs.mock("@/core/extensions/hooks", () => ({
  useFrontendServices: () => ({
    conversationText: rs.fn(),
    showMessage: rs.fn(),
  }),
}));
rs.mock("@/core/files", () => ({
  useSaveToMyFiles: () => ({ save: rs.fn(), isPending: false }),
}));
rs.mock("@/core/shared", () => ({
  useShareWithEveryone: () => ({ share: rs.fn(), isPending: false }),
}));
rs.mock("@/core/artifact-views/exports", () => ({
  useLiveArtifactExports: ({ eligible }: { eligible: unknown[] }) => ({
    files: eligible,
    settled: true,
    uncertain: false,
    checking: false,
    retry: rs.fn(),
  }),
}));

const path = "/mnt/user-data/outputs/results/a.summary.json";
const presentation: InstalledArtifactPresentation = {
  contribution: {
    namespace: "example.summary",
    viewer_id: "viewer-1",
    module: "summary.v1",
    entry: `/api/plugins/modules/summary.v1/${"a".repeat(64)}.mjs`,
    title: "Summary",
    description: "",
    settings: { enabled: true },
  },
  descriptor: {
    id: "summary",
    suffixes: [".summary.json"],
    source_max_bytes: 1024 * 1024,
    preview_max_bytes: 1024 * 1024,
    projection_marker: null,
  },
  surface: {
    id: "summary",
    title: "Summary",
    kind: "native",
    mount: (root) => {
      root.textContent = "Native summary";
      return { dispose: () => undefined };
    },
  },
};
const props = {
  installation: presentation,
  content: "{}",
  revision: "a".repeat(64),
  filepath: path,
  threadId: "thread-1",
  viewerId: "viewer-1",
  projected: false,
  artifacts: [path, "/mnt/user-data/outputs/results/a.pdf"],
  presentedKnown: true,
  runSettled: true,
  onUnavailable: () => undefined,
};

describe("installed artifact mount lifecycle", () => {
  afterEach(() => {
    cleanup();
    rs.restoreAllMocks();
  });
  it("mounts independent DOM and disposes StrictMode replay and unmount", () => {
    const dispose = rs.fn();
    const contexts: ArtifactSurfaceContext[] = [];
    const native = {
      ...presentation,
      surface: {
        ...presentation.surface,
        kind: "native" as const,
        mount: (root: HTMLElement, context: ArtifactSurfaceContext) => {
          contexts.push(context);
          root.textContent = "Native summary";
          return { dispose };
        },
      },
    };
    const { unmount } = render(
      <StrictMode>
        <PluginArtifactPresentation {...props} installation={native} />
      </StrictMode>,
    );
    expect(
      screen.getByTestId("plugin-artifact-presentation").firstElementChild
        ?.shadowRoot?.textContent,
    ).toBe("Native summary");
    expect(contexts).toHaveLength(2);
    expect(contexts[0]!.signal.aborted).toBe(true);
    expect(contexts[1]!.signal.aborted).toBe(false);
    expect(dispose).toHaveBeenCalledTimes(1);
    unmount();
    expect(contexts[1]!.signal.aborted).toBe(true);
    expect(dispose).toHaveBeenCalledTimes(2);
  });
  it("retires a changed source revision and rejects late namespace service calls", async () => {
    let context!: ArtifactSurfaceContext;
    const dispose = rs.fn();
    const native = {
      ...presentation,
      surface: {
        id: "summary",
        title: "Summary",
        kind: "native" as const,
        mount: (root: HTMLElement, next: ArtifactSurfaceContext) => {
          context = next;
          root.textContent = next.artifact.revision;
          return { dispose };
        },
      },
    };
    const { rerender } = render(
      <PluginArtifactPresentation {...props} installation={native} />,
    );
    const old = context;
    rerender(
      <PluginArtifactPresentation
        {...props}
        installation={native}
        revision={"b".repeat(64)}
      />,
    );
    expect(old.signal.aborted).toBe(true);
    await expect(old.callBackend("later", {})).rejects.toThrow();
    expect(context.signal.aborted).toBe(false);
    expect(context.artifact.revision).toBe("b".repeat(64));
    expect(dispose).toHaveBeenCalledTimes(1);
  });
  it.each(["thread", "path", "module", "viewer"])(
    "disposes and aborts the old native mount on %s change",
    (change) => {
      const contexts: ArtifactSurfaceContext[] = [];
      const dispose = rs.fn();
      const native = {
        ...presentation,
        surface: {
          id: "summary",
          title: "Summary",
          kind: "native" as const,
          mount: (_root: HTMLElement, context: ArtifactSurfaceContext) => {
            contexts.push(context);
            return { dispose };
          },
        },
      };
      const { rerender } = render(
        <PluginArtifactPresentation {...props} installation={native} />,
      );
      const next = {
        ...props,
        installation: native,
        ...(change === "thread" ? { threadId: "thread-2" } : {}),
        ...(change === "path"
          ? { filepath: "/mnt/user-data/outputs/results/b.summary.json" }
          : {}),
        ...(change === "module"
          ? {
              installation: {
                ...native,
                contribution: {
                  ...native.contribution,
                  entry: `/api/plugins/modules/summary.v1/${"b".repeat(64)}.mjs`,
                },
              },
            }
          : {}),
        ...(change === "viewer"
          ? {
              viewerId: "viewer-2",
              installation: {
                ...native,
                contribution: { ...native.contribution, viewer_id: "viewer-2" },
              },
            }
          : {}),
      };
      rerender(<PluginArtifactPresentation {...next} />);
      expect(contexts).toHaveLength(2);
      expect(contexts[0]!.signal.aborted).toBe(true);
      expect(contexts[1]!.signal.aborted).toBe(false);
      expect(dispose).toHaveBeenCalledTimes(1);
    },
  );
  it("does not commit a passive result after account retirement", async () => {
    let finish!: (value: unknown) => void;
    const pending = new Promise<never>((resolve) => {
      finish = resolve as (value: unknown) => void;
    });
    const unavailable = rs.fn();
    const passive = {
      ...presentation,
      surface: {
        id: "summary",
        title: "Summary",
        kind: "passive" as const,
        present: () => pending,
      },
    };
    const { unmount } = render(
      <PluginArtifactPresentation
        {...props}
        installation={passive}
        onUnavailable={unavailable}
      />,
    );
    unmount();
    finish({
      format: "hartmesh.artifact-view",
      version: 1,
      title: "Late account result",
      blocks: [],
      exports: [],
    });
    await pending;
    await Promise.resolve();
    expect(screen.queryByText("Late account result")).toBeNull();
    expect(unavailable).not.toHaveBeenCalled();
  });
  it("draws a passive return through the strict generic host and recorded exports", async () => {
    const passive = {
      ...presentation,
      surface: {
        id: "summary",
        title: "Summary",
        kind: "passive" as const,
        present: () => ({
          format: "hartmesh.artifact-view" as const,
          version: 1 as const,
          title: "Rendered summary",
          blocks: [
            { type: "text" as const, paragraphs: ["<script>literal</script>"] },
          ],
          exports: [
            { path: "a.pdf", label: "Printable summary" },
            { path: "not-presented.pdf", label: "Not presented" },
          ],
        }),
      },
    };
    render(<PluginArtifactPresentation {...props} installation={passive} />);
    await waitFor(() =>
      expect(screen.getByText("Rendered summary")).toBeDefined(),
    );
    expect(screen.getByText("<script>literal</script>")).toBeDefined();
    expect(
      screen.getByRole("link", { name: "Download Printable summary" }),
    ).toBeDefined();
    expect(screen.queryByText("Not presented")).toBeNull();
  });
  it("contains mount failure and rejects invalid file references without orphaning a controller", () => {
    const unavailable = rs.fn();
    const dispose = rs.fn();
    const native = {
      ...presentation,
      surface: {
        id: "summary",
        title: "Summary",
        kind: "native" as const,
        mount: (root: HTMLElement) => {
          root.textContent = "partial";
          return {
            dispose,
            files: { exports: [{ path: "../outside.pdf", label: "Bad" }] },
          };
        },
      },
    };
    render(
      <PluginArtifactPresentation
        {...props}
        installation={native}
        onUnavailable={unavailable}
      />,
    );
    expect(unavailable).toHaveBeenCalledTimes(1);
    expect(dispose).toHaveBeenCalledTimes(1);
    expect(
      screen.getByTestId("plugin-artifact-presentation").firstElementChild
        ?.shadowRoot?.textContent,
    ).toBe("");
  });
});
