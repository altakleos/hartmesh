import { afterEach, beforeEach, expect, test, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";

const state = rs.hoisted(() => ({
  select: rs.fn(),
  setOpen: rs.fn(),
  manifest: rs.fn(),
  backend: "",
  image: "/mnt/user-data/outputs/chart.png",
}));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => state.backend }));
rs.mock("@/components/workspace/artifacts/context", () => ({
  useArtifacts: () => ({ select: state.select, setOpen: state.setOpen }),
}));
rs.mock("@/core/features/hooks", () => ({
  useCustomerAdministration: () => ({ localSkillManagement: true }),
}));
rs.mock("@/core/artifacts/api", () => ({
  getArtifactArchiveManifest: state.manifest,
  MAX_ARTIFACT_ARCHIVE_FILES: 50,
  downloadArtifactArchive: rs.fn(),
  ArtifactRequestError: class extends Error {},
}));
rs.mock("@/components/workspace/messages/markdown-content", () => ({
  MarkdownContent: ({
    components,
  }: {
    components: { img: (props: Record<string, string>) => React.ReactNode };
  }) => components.img({ src: state.image, alt: "Chart" }),
}));

import { ArtifactFileList } from "@/components/workspace/artifacts/artifact-file-list";
import { ConversationFilesContext } from "@/components/workspace/chats/conversation-files";
import { createMarkdownLinkComponent } from "@/components/workspace/messages/markdown-link";
import { MessageListItem } from "@/components/workspace/messages/message-list-item";
import { I18nProvider } from "@/core/i18n/context";

const clients: QueryClient[] = [];
const sentence = "Ask a manager for access.";
const binding = {
  id: "a".repeat(32),
  name: "Researcher",
  status: "active" as const,
  custody: "personal" as const,
  requester_id: "reader",
};
beforeEach(() => {
  state.backend = "";
  state.image = "/mnt/user-data/outputs/chart.png";
  state.select.mockReset();
  state.setOpen.mockReset();
  state.manifest.mockReset().mockResolvedValue({ fileCount: 2 });
});
afterEach(() => {
  cleanup();
  clients.splice(0).forEach((client) => client.clear());
});
function show(
  locked: boolean,
  notice: string | null,
  agent: typeof binding | null = binding,
) {
  const client = new QueryClient();
  clients.push(client);
  return render(
    <QueryClientProvider client={client}>
      <I18nProvider initialLocale="en-US">
        <ConversationFilesContext.Provider
          value={{ locked, sentence: notice, agent }}
        >
          <ArtifactFileList
            files={[
              "/mnt/user-data/outputs/report.pdf",
              "/mnt/user-data/outputs/reviewer.skill",
            ]}
            threadId="thread"
            runId="run"
          />
        </ConversationFilesContext.Provider>
      </I18nProvider>
    </QueryClientProvider>,
  );
}
test("locked produced files keep names but offer no open, download, install or archive request", () => {
  show(true, sentence);
  expect(screen.getByText("report.pdf")).toBeDefined();
  fireEvent.click(screen.getByText("report.pdf"));
  expect(state.select).not.toHaveBeenCalled();
  expect(screen.queryByRole("link")).toBeNull();
  expect(screen.queryByRole("button", { name: "Install" })).toBeNull();
  expect(screen.getAllByText(sentence)).toHaveLength(1);
  expect(state.manifest).not.toHaveBeenCalled();
});
test("pending file authority stays locked without fabricating an explanatory sentence", () => {
  show(true, null);
  fireEvent.click(screen.getByText("report.pdf"));
  expect(state.select).not.toHaveBeenCalled();
  expect(screen.queryByRole("link")).toBeNull();
  expect(state.manifest).not.toHaveBeenCalled();
});
test("an inspector can open individual agent files but never requests an unsupported archive", () => {
  show(false, null);
  fireEvent.click(screen.getByText("report.pdf"));
  expect(state.select).not.toHaveBeenCalled();
  expect(
    screen.getByRole("link", { name: "report.pdf" }).getAttribute("href"),
  ).toContain("/api/threads/thread/artifacts/mnt/user-data/outputs/report.pdf");
  expect(screen.getAllByRole("link", { name: "Download" })).toHaveLength(2);
  expect(state.manifest).not.toHaveBeenCalled();
});
test("file links in Markdown are inert under the same current authority without blocking external citations", () => {
  const Link = createMarkdownLinkComponent("thread");
  render(
    <ConversationFilesContext.Provider
      value={{ locked: true, sentence, agent: binding }}
    >
      <Link href="/mnt/user-data/outputs/report.pdf">Report</Link>
      <Link href="https://example.test/reference">Reference</Link>
    </ConversationFilesContext.Provider>,
  );
  expect(screen.queryByRole("link", { name: "Report" })).toBeNull();
  expect(screen.getByText("Report").getAttribute("title")).toBe(sentence);
  expect(screen.getByRole("link", { name: "Reference" })).toBeDefined();
});
test("an inline file image renders alternative text without an image request when inspection is unavailable", () => {
  render(
    <I18nProvider initialLocale="en-US">
      <ConversationFilesContext.Provider
        value={{ locked: true, sentence, agent: binding }}
      >
        <MessageListItem
          message={{ type: "ai", id: "answer", content: "Chart" }}
          threadId="thread"
          showCopyButton={false}
        />
      </ConversationFilesContext.Provider>
    </I18nProvider>,
  );
  expect(document.querySelector("img")).toBeNull();
  expect(screen.getByText("Chart").getAttribute("title")).toBe(sentence);
});
test("a configured backend path prefix keeps resolved file images and full file links locked", () => {
  state.backend = "https://gateway.test/backend";
  state.image = `${state.backend}/api/threads/thread/artifacts/mnt/user-data/outputs/chart.png`;
  const Link = createMarkdownLinkComponent("thread");
  render(
    <I18nProvider initialLocale="en-US">
      <ConversationFilesContext.Provider
        value={{ locked: true, sentence, agent: binding }}
      >
        <MessageListItem
          message={{ type: "ai", id: "answer", content: "Chart" }}
          threadId="thread"
          showCopyButton={false}
        />
        <Link
          href={`${state.backend}/api/threads/thread/artifacts/mnt/user-data/outputs/report.pdf`}
        >
          Report
        </Link>
        <Link href="https://reference.test/backend/api/threads/thread/artifacts/reference">
          External reference
        </Link>
      </ConversationFilesContext.Provider>
    </I18nProvider>,
  );
  expect(document.querySelector("img")).toBeNull();
  expect(screen.queryByRole("link", { name: "Report" })).toBeNull();
  expect(
    screen.getByRole("link", { name: "External reference" }),
  ).toBeDefined();
});
test("an image attached to an agent conversation is a file chip instead of an unsupported inline upload image", () => {
  render(
    <I18nProvider initialLocale="en-US">
      <ConversationFilesContext.Provider
        value={{ locked: false, sentence: null, agent: binding }}
      >
        <MessageListItem
          message={{
            type: "human",
            id: "question",
            content: "Read this",
            additional_kwargs: {
              files: [
                {
                  filename: "scan.png",
                  size: 12,
                  path: "/mnt/user-data/uploads/scan.png",
                  status: "uploaded",
                },
              ],
            },
          }}
          threadId="thread"
          showCopyButton={false}
        />
      </ConversationFilesContext.Provider>
    </I18nProvider>,
  );
  expect(document.querySelector("img")).toBeNull();
  expect(
    screen.getByRole("link", { name: "scan.png" }).getAttribute("href"),
  ).toBe(
    `/workspace/instances?instance=${binding.id}&tab=files&folder=uploads`,
  );
});

test("an attached image stays inert while the thread binding itself is still unknown", () => {
  render(
    <I18nProvider initialLocale="en-US">
      <ConversationFilesContext.Provider
        value={{ locked: true, sentence: null, agent: null }}
      >
        <MessageListItem
          message={{
            type: "human",
            id: "question",
            content: "Read this",
            additional_kwargs: {
              files: [
                {
                  filename: "scan.png",
                  size: 12,
                  path: "/mnt/user-data/uploads/scan.png",
                  status: "uploaded",
                },
              ],
            },
          }}
          threadId="thread"
          showCopyButton={false}
        />
      </ConversationFilesContext.Provider>
    </I18nProvider>,
  );
  expect(document.querySelector("img")).toBeNull();
  expect(screen.queryByRole("link", { name: "scan.png" })).toBeNull();
});
