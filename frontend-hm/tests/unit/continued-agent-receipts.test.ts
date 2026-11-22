import { describe, expect, it } from "@rstest/core";
import { QueryClient } from "@tanstack/react-query";

import {
  saveContinuedDraft,
  type ContinuedDraft,
} from "@/core/shared-conversations/handoff";
import {
  validAnswerer,
  type ContinuedReceipt,
  type ContinueResult,
} from "@/core/shared-conversations/protocol";

const returned: ContinuedReceipt = {
  filename: "report (2).pdf",
  size: 17,
  virtual_path: "/mnt/user-data/uploads/report (2).pdf",
  sha256: "e".repeat(64),
  source: {
    kind: "shared_publication",
    publication_id: "member",
    sha256: "e".repeat(64),
  },
};
const operation = (attachment: ContinuedReceipt): ContinueResult => ({
  answerer: { kind: "agent", id: "employee", name: "Maya" },
  thread_id: "private-thread",
  files: [{ path: "report.pdf", status: "copied", attachment }],
});
describe("Continue into an agent conversation", () => {
  it("requires the actual source proof and returned Home path before storing a composer attachment", () => {
    const client = new QueryClient();
    for (const receipt of [
      { ...returned, source: undefined },
      { ...returned, source: { ...returned.source!, sha256: "f".repeat(64) } },
      { ...returned, virtual_path: "/mnt/user-data/uploads/report.pdf" },
    ]) {
      expect(() =>
        saveContinuedDraft(
          client,
          "member",
          "manifest",
          "request",
          "Review",
          operation(receipt),
        ),
      ).toThrow();
    }
    const key = ["shared-conversation-draft", "member", "private-thread"];
    expect(client.getQueryData(key)).toBeUndefined();
    saveContinuedDraft(
      client,
      "member",
      "manifest",
      "request",
      "Review",
      operation(returned),
    );
    expect(client.getQueryData<ContinuedDraft>(key)).toMatchObject({
      answerer: { kind: "agent", id: "employee" },
      files: [{ source: "report.pdf", receipt: returned }],
    });
    expect(
      client.getQueryData([
        "shared-conversation-draft",
        "other",
        "private-thread",
      ]),
    ).toBeUndefined();
  });
  it("accepts all negotiated fallback reasons and excludes incomplete or future answerer forms", () => {
    for (const reason of ["no_access", "paused", "retired", "unavailable"]) {
      expect(validAnswerer({ kind: "assistant", reason })).toBe(true);
    }
    expect(validAnswerer({ kind: "assistant" })).toBe(true);
    expect(validAnswerer({ kind: "assistant", reason: "new-reason" })).toBe(
      false,
    );
    expect(validAnswerer({ kind: "agent", id: "employee" })).toBe(false);
    expect(validAnswerer({ kind: "agent", name: "Maya" })).toBe(false);
  });
});
