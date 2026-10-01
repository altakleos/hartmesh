import { afterEach, beforeEach, describe, expect, it, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";

const myFilesSave = rs.hoisted(() =>
  rs.fn<(paths: readonly string[], folder?: string) => Promise<unknown[]>>(),
);
const shareWithEveryone = rs.hoisted(() =>
  rs.fn<(paths: readonly string[], folder?: string) => Promise<unknown[]>>(),
);
const artifacts = rs.hoisted(() => ({ list: [] as string[] }));

rs.mock("sonner", () => ({ toast: { success: rs.fn(), error: rs.fn() } }));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({ user: { system_role: "user" } }),
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
  useThread: () => ({ thread: { isLoading: false }, isMock: false }),
}));

import { ArtifactFileDetail } from "@/components/workspace/artifacts/artifact-file-detail";
import { I18nContext } from "@/core/i18n/context";
import { enUS } from "@/core/i18n/locales/en-US";

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
  });

  afterEach(cleanup);

  it("files a report's download with the reports, as the report card does", () => {
    // Same file, another way in: the panel must not put a second copy of one
    // report at the root while the card files it under Reports.
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
});
